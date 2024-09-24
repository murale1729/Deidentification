#!/usr/bin/env python3

import argparse
import os
import shutil
import struct
import pandas as pd
import tiffparser
import time  # Import time module
from datetime import datetime  # For timestamp

# Dictionary to track unique ID for each folder
folder_file_count = {}

# Counters for tracking success and failure
total_files = 0
successful_files = 0
failed_files = 0

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

def log_file_update(log_file, svs_file, new_filename, status, time_taken, input_folder, output_folder, completion_percentage):
    """Update the log file with the current file's processing details, including time taken, timestamp, and folders."""
    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_dict = {
        'Timestamp': timestamp,
        'Original_file_name': svs_file,
        'Deidentified_file_name': new_filename,
        'Status': status,
        'Time_Taken_Seconds': time_taken,  # Add time taken in seconds
        'Input_Folder': input_folder,  # Log the input folder
        'Output_Folder': output_folder,  # Log the output folder
    }

    log_df = pd.DataFrame([log_dict])
    log_df.to_csv(log_file, mode='a', header=not os.path.exists(log_file), index=False)

def generate_unique_filename(folder_name, ext):
    """Generate a unique filename with 'DI' and increasing ID based on folder name."""
    if folder_name not in folder_file_count:
        folder_file_count[folder_name] = 0

    folder_file_count[folder_name] += 1

    # Create a unique filename with 'DI' prefix, folder name, and an increasing ID
    unique_id = folder_file_count[folder_name]
    new_filename = f"DI_{folder_name}_{unique_id:04d}{ext}"  # Example: DI_foldername_0001.svs
    return new_filename

def deidentify_svs_file(input_file, temp_input_file, temp_output_file, log_file, input_folder, output_folder, folder_name, file_index, total_files):
    """Deidentify a single SVS file using temporary directories."""
    global successful_files, failed_files

    start_time = time.time()  # Start time
    try:
        print(f"Copying input file to temporary input folder:\n  Source: {input_file}\n  Destination: {temp_input_file}")
        shutil.copy(input_file, temp_input_file)

        print(f"Copying file from temporary input to temporary output folder:\n  Source: {temp_input_file}\n  Destination: {temp_output_file}")
        shutil.copy(temp_input_file, temp_output_file)

        print(f"Deidentifying file: {temp_output_file}")
        delete_associated_image(temp_output_file, 'label')
        delete_associated_image(temp_output_file, 'macro')

        print(f"Deidentification completed for: {temp_output_file}")

        end_time = time.time()  # End time
        time_taken = end_time - start_time  # Calculate time taken

        # Generate unique filename for the output file
        ext = os.path.splitext(input_file)[1]  # Get file extension (e.g., .svs)
        unique_filename = generate_unique_filename(folder_name, ext)

        # Calculate percentage completion
        completion_percentage = (file_index / total_files) * 100

        # Log the result
        log_file_update(log_file, os.path.basename(input_file), unique_filename, 'Success', time_taken, input_folder, output_folder)

        successful_files += 1
    except Exception as e:
        print(f"Failed to deidentify {input_file}: {e}")
        end_time = time.time()  # End time in case of failure
        time_taken = end_time - start_time  # Calculate time taken
        completion_percentage = (file_index / total_files) * 100
        log_file_update(log_file, os.path.basename(input_file), os.path.basename(temp_output_file), f'Failed: {e}', time_taken, input_folder, output_folder, completion_percentage)
        failed_files += 1

def process_svs_files(input_dir, output_dir, temp_dir, log_file):
    """Process all SVS files in the input directory using user-specified temporary folders."""
    global total_files

    # Count total .svs files for progress tracking
    total_files = sum(len(files) for _, _, files in os.walk(input_dir) if any(f.lower().endswith('.svs') for f in files))

    if total_files == 0:
        print("No SVS files found.")
        return

    temp_input_folder = os.path.join(temp_dir, 'temp_input_folder')
    temp_output_folder = os.path.join(temp_dir, 'temp_output_folder')

    # Create temporary folders if they don't exist
    os.makedirs(temp_input_folder, exist_ok=True)
    os.makedirs(temp_output_folder, exist_ok=True)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

    file_index = 1  # To track the progress

    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.lower().endswith('.svs'):
                input_file = os.path.join(root, filename)

                # Define paths in temporary folders
                temp_input_file = os.path.join(temp_input_folder, filename)
                temp_output_file = os.path.join(temp_output_folder, filename)

                print(f"\nProcessing file: {input_file}")

                # Extract the folder name to use for unique ID generation
                folder_name = os.path.basename(input_dir)

                # Deidentify the file using temporary folders
                deidentify_svs_file(input_file, temp_input_file, temp_output_file, log_file, root, output_dir, folder_name, file_index, total_files)

                # Build output file path
                relative_path = os.path.relpath(root, input_dir)
                output_subdir = os.path.join(output_dir, relative_path)
                if not os.path.exists(output_subdir):
                    os.makedirs(output_subdir)

                # Generate the unique filename
                ext = os.path.splitext(filename)[1]
                unique_filename = generate_unique_filename(folder_name, ext)

                output_file = os.path.join(output_subdir, unique_filename)

                print(f"Moving deidentified file to final output directory:\n  Source: {temp_output_file}\n  Destination: {output_file}")
                shutil.move(temp_output_file, output_file)

                print(f"Deleting temporary input file: {temp_input_file}")
                os.remove(temp_input_file)

                print(f"Finished processing file: {input_file}")
                print("-" * 60)

                

                file_index += 1
                completion_percentage = (file_index / total_files) * 100
                print(f"{completion_percentage:.2f}% completed")

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

    # Print transfer stats
    print(f"\nTransfer Stats: Successful Files: {successful_files}, Failed Files: {failed_files}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deidentify SVS files in a directory using user-specified temporary folders')
    parser.add_argument('--input_dir', required=True, help='Input directory containing SVS files')
    parser.add_argument('--output_dir', required=True, help='Output directory to save deidentified SVS files')
    parser.add_argument('--temp_dir', required=True, help='Temporary directory to store intermediate files')
    parser.add_argument('--log_file', required=True, help='Log file path')
    args = parser.parse_args()

    process_svs_files(args.input_dir, args.output_dir, args.temp_dir, args.log_file)
