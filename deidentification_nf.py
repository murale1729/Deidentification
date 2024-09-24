import argparse
import os
import shutil
import struct
import tiffparser
import pandas as pd
from datetime import datetime

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

def log_file_update(log_file, svs_file, new_filename, input_path, output_path, status, prefix):
    """Update the log file with the current file's processing details."""
    # Get the folder name (parent directory of the input file)
    folder_name = os.path.basename(os.path.dirname(input_path))
    
    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_dict = {
        'Timestamp': timestamp,
        'Folder': folder_name,
        'Original_file_name': svs_file,
        'Deidentified_file_name': new_filename,
        'Prefix': prefix,
        'Input_Path': input_path,
        'Output_Path': output_path,
        'Status': status
    }

    log_df = pd.DataFrame([log_dict])
    log_df.to_csv(log_file, mode='a', header=not os.path.exists(log_file), index=False)

def deidentify_svs_files(input_file, output_file, log_file, prefix):
    """Deidentify a single SVS file with a prefix."""
    try:
        # Apply the prefix to the output file name
        output_dir = os.path.dirname(output_file)
        output_filename = os.path.basename(output_file)
        prefixed_output_file = os.path.join(output_dir, f"{prefix}_{output_filename}")

        # Copy the original file to the prefixed output file path (simulating the deidentification process)
        shutil.copy(input_file, prefixed_output_file)

        # Remove label and macro images
        delete_associated_image(prefixed_output_file, 'label')
        delete_associated_image(prefixed_output_file, 'macro')
        print(f"Deidentified {input_file} successfully.")

        # Log success
        log_file_update(
            log_file, 
            os.path.basename(input_file), 
            os.path.basename(prefixed_output_file), 
            input_file, 
            prefixed_output_file, 
            'Success',
            prefix
        )
    except Exception as e:
        print(f"Failed to deidentify {input_file}: {e}")
        # Log failure
        log_file_update(
            log_file, 
            os.path.basename(input_file), 
            os.path.basename(output_file), 
            input_file, 
            output_file, 
            f'Failed: {e}',
            prefix
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deidentify SVS files with a prefix')
    parser.add_argument('--input', required=True, help='Input file path')
    parser.add_argument('--output', required=True, help='Output file path')
    parser.add_argument('--log', required=True, help='Log file path')
    parser.add_argument('--prefix', required=True, help='Prefix to be added to the output file name')
    args = parser.parse_args()

    deidentify_svs_files(args.input, args.output, args.log, args.prefix)
