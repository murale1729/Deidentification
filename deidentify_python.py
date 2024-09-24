#!/usr/bin/env python3

import argparse
import os
import shutil
import struct
import pandas as pd
import tiffparser

def delete_associated_image(slide_path, image_type):
    """Remove label or macro image from a given SVS file."""
    allowed_image_types = ['label', 'macro']
    if image_type not in allowed_image_types:
        raise Exception('Invalid image type requested for deletion')

    with open(slide_path, 'r+b') as fp:
        t = tiffparser.TiffFile(fp)

        filtered_pages = [page for page in t.pages if image_type in page.description]
        if len(filtered_pages) > 1:
            raise Exception(f'Duplicate associated {image_type} images found in the file.')
        if len(filtered_pages) == 0:
            return  # No image of this type to delete

        page = filtered_pages[0]

        # IFD management
        offsetformat = t.tiff.ifdoffsetformat
        offsetsize = t.tiff.ifdoffsetsize
        tagnoformat = t.tiff.tagnoformat
        tagnosize = t.tiff.tagnosize
        tagsize = t.tiff.tagsize
        unpack = struct.unpack

        ifds = [{'this': p.offset} for p in t.pages]
        for p in ifds:
            fp.seek(p['this'])
            (num_tags,) = unpack(tagnoformat, fp.read(tagnosize))
            fp.seek(num_tags * tagsize, 1)
            p['next_ifd_offset'] = fp.tell()
            (p['next_ifd_value'],) = unpack(offsetformat, fp.read(offsetsize))

        pageifd = [i for i in ifds if i['this'] == page.offset][0]
        previfd = [i for i in ifds if i['next_ifd_value'] == page.offset]
        if len(previfd) == 0:
            raise Exception('No page points to this one')
        else:
            previfd = previfd[0]

        # Erase image data
        offsets = page.tags['StripOffsets'].value
        bytecounts = page.tags['StripByteCounts'].value
        for (o, b) in zip(offsets, bytecounts):
            fp.seek(o)
            fp.write(b'\0' * b)

        # Erase tag values
        for key, tag in page.tags.items():
            fp.seek(tag.valueoffset)
            fp.write(b'\0' * tag.count)

        pagebytes = (pageifd['next_ifd_offset'] - pageifd['this']) + offsetsize

        # Zero out the page header
        fp.seek(pageifd['this'])
        fp.write(b'\0' * pagebytes)

        # Update the previous page's IFD to skip this page
        fp.seek(previfd['next_ifd_offset'])
        fp.write(struct.pack(offsetformat, pageifd['next_ifd_value']))

def log_file_update(log_file, svs_file, new_filename, status):
    """Update the log file with the current file's processing details."""
    log_dict = {
        'Original_file_name': svs_file,
        'Deidentified_file_name': new_filename,
        'Status': status
    }

    log_df = pd.DataFrame([log_dict])
    log_df.to_csv(log_file, mode='a', header=not os.path.exists(log_file), index=False)

def deidentify_svs_file(input_file, temp_input_file, temp_output_file, log_file):
    """Deidentify a single SVS file using temporary directories."""
    try:
        print(f"Copying input file to temporary input folder:\n  Source: {input_file}\n  Destination: {temp_input_file}")
        # Copy input file to temporary input folder
        shutil.copy(input_file, temp_input_file)

        print(f"Copying file from temporary input to temporary output folder:\n  Source: {temp_input_file}\n  Destination: {temp_output_file}")
        # Copy temp input file to temp output file (deidentify in place)
        shutil.copy(temp_input_file, temp_output_file)

        # Deidentify the file in the temp output folder
        print(f"Deidentifying file: {temp_output_file}")
        delete_associated_image(temp_output_file, 'label')
        delete_associated_image(temp_output_file, 'macro')

        print(f"Deidentification completed for: {temp_output_file}")

        log_file_update(log_file, os.path.basename(input_file), os.path.basename(temp_output_file), 'Success')
    except Exception as e:
        print(f"Failed to deidentify {input_file}: {e}")
        log_file_update(log_file, os.path.basename(input_file), os.path.basename(temp_output_file), f'Failed: {e}')

def process_svs_files(input_dir, output_dir, log_file):
    """Process all SVS files in the input directory using temporary folders."""
    temp_input_folder = os.path.join(output_dir, 'temp_input_folder')
    temp_output_folder = os.path.join(output_dir, 'temp_output_folder')

    # Create temporary folders if they don't exist
    os.makedirs(temp_input_folder, exist_ok=True)
    os.makedirs(temp_output_folder, exist_ok=True)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.lower().endswith('.svs'):
                input_file = os.path.join(root, filename)

                # Define paths in temporary folders
                temp_input_file = os.path.join(temp_input_folder, filename)
                temp_output_file = os.path.join(temp_output_folder, filename)

                print(f"\nProcessing file: {input_file}")

                # Deidentify the file using temporary folders
                deidentify_svs_file(input_file, temp_input_file, temp_output_file, log_file)

                # Build output file path
                # Retain the directory structure relative to input_dir
                relative_path = os.path.relpath(root, input_dir)
                output_subdir = os.path.join(output_dir, relative_path)
                if not os.path.exists(output_subdir):
                    os.makedirs(output_subdir)

                output_file = os.path.join(output_subdir, filename)

                print(f"Moving deidentified file to final output directory:\n  Source: {temp_output_file}\n  Destination: {output_file}")
                # Move the deidentified file to the final output directory
                shutil.move(temp_output_file, output_file)

                print(f"Deleting temporary input file: {temp_input_file}")
                # Remove the temporary input file
                os.remove(temp_input_file)

                print(f"Finished processing file: {input_file}")
                print("-" * 60)

    # Clean up temporary folders (remove only if empty)
    try:
        os.rmdir(temp_input_folder)
        print(f"Removed temporary input folder: {temp_input_folder}")
    except OSError:
        print(f"Temporary input folder not empty or could not be removed: {temp_input_folder}")

    try:
        os.rmdir(temp_output_folder)
        print(f"Removed temporary output folder: {temp_output_folder}")
    except OSError:
        print(f"Temporary output folder not empty or could not be removed: {temp_output_folder}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deidentify SVS files in a directory using temporary folders')
    parser.add_argument('--input_dir', required=True, help='Input directory containing SVS files')
    parser.add_argument('--output_dir', required=True, help='Output directory to save deidentified SVS files')
    parser.add_argument('--log_file', required=True, help='Log file path')
    args = parser.parse_args()

    process_svs_files(args.input_dir, args.output_dir, args.log_file)
