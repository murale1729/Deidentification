#!/usr/bin/env python3

import argparse
import os
import shutil
import struct
import pandas as pd
import tiffparser
import time
from datetime import datetime
import subprocess

# Dictionary to track unique ID for each folder
folder_file_count = {}

# Counters for tracking success and failure
successful_files = 0
failed_files = 0

def delete_associated_image(slide_path, image_type):
    """Remove label or macro image from a given SVS file."""
    allowed_image_types = ['label', 'macro']
    if image_type not in allowed image_types:
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

def log_file_update(log_file, svs_file, new_filename, status, time_taken, input_folder, output_folder):
    """Update the log file with the current file's processing details, including time taken, timestamp, and folders."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_dict = {
        'Timestamp': timestamp,
        'Original_file_name': svs_file,
        'Deidentified_file_name': new_filename,
        'Status': status,
        'Time_Taken_Seconds': time_taken,
        'Input_Folder': input_folder,
        'Output_Folder': output_folder
    }

    log_df = pd.DataFrame([log_dict])
    log_df.to_csv(log_file, mode='a', header=not os.path.exists(log_file), index=False)

def generate_unique_filename(folder_name, ext):
    """Generate a unique filename with 'DI' and increasing ID based on folder name."""
    if folder_name not in folder_file_count:
        folder_file_count[folder_name] = 0

    folder_file_count[folder_name] += 1

    unique_id = folder_file_count[folder_name]
    new_filename = f"DI_{folder_name}_{unique_id:04d}{ext}"
    return new_filename

def deidentify_svs_file(input_file, temp_input_file, temp_output_file, log_file, input_folder, output_folder, folder_name):
    """Deidentify a single SVS file using temporary directories."""
    global successful_files, failed_files

    start_time = time.time()  # Start time
    try:
        print(f"Deidentifying file: {temp_output_file}")
        delete_associated_image(temp_output_file, 'label')
        delete_associated_image(temp_output_file, 'macro')

        print(f"Deidentification completed for: {temp_output_file}")

        end_time = time.time()
        time_taken = end_time - start_time

        ext = os.path.splitext(input_file)[1]
        unique_filename = generate_unique_filename(folder_name, ext)

        log_file_update(log_file, os.path.basename(input_file), unique_filename, 'Success', time_taken, input_folder, output_folder)

        successful_files += 1

    except Exception as e:
        print(f"Failed to deidentify {input_file}: {e}")
        end_time = time.time()
        time_taken = end_time - start_time
        log_file_update(log_file, os.path.basename(input_file), os.path.basename(temp_output_file), f'Failed: {e}', time_taken, input_folder, output_folder)
        failed_files += 1

def process_single_svs_file(s3_bucket_input, s3_bucket_output, temp_dir, log_file):
    """Process each SVS file one by one from the input S3 bucket."""
    global successful_files, failed_files

    temp_input_folder = os.path.join(temp_dir, 'domboxDec2016')
    temp_output_folder = os.path.join(temp_dir, 'temp_output')

    os.makedirs(temp_input_folder, exist_ok=True)
    os.makedirs(temp_output_folder, exist_ok=True)

    # Ensure s3_bucket_input and s3_bucket_output have correct prefixes
    if not s3_bucket_input.startswith('s3://'):
        s3_bucket_input = 's3://' + s3_bucket_input.rstrip('/')
    else:
        s3_bucket_input = s3_bucket_input.rstrip('/')

    if not s3_bucket_output.startswith('s3://'):
        s3_bucket_output = 's3://' + s3_bucket_output.rstrip('/')
    else:
        s3_bucket_output = s3_bucket_output.rstrip('/')

    # List files in the S3 bucket
    try:
        result = subprocess.run(['aws', 's3', 'ls', s3_bucket_input + '/'], capture_output=True, text=True, check=True, timeout=60)
        # Handle spaces in filenames by splitting with maxsplit=3 to get only 4 parts (date, time, size, filename)
        svs_files = [line.split(maxsplit=3)[-1] for line in result.stdout.splitlines() if line.strip().endswith('.svs')]
    except subprocess.TimeoutExpired:
        print(f"Timeout occurred while listing files in {s3_bucket_input}")
        return
    except subprocess.CalledProcessError as e:
        print(f"Error listing files from S3: {e}")
        return

    if not svs_files:
        print("No SVS files found.")
        return

    for svs_file in svs_files:
        # Download the file from S3
        input_file = os.path.join(temp_input_folder, svs_file)
        try:
            s3_file_path = f"{s3_bucket_input}/{svs_file}"

            # Check if the file exists in S3 before trying to download
            check_result = subprocess.run(['aws', 's3', 'ls', s3_file_path], capture_output=True, text=True, timeout=60)
            if not check_result.stdout:
                print(f"File not found in S3: {s3_file_path}")
                continue

            subprocess.run(['aws', 's3', 'cp', s3_file_path, input_file], check=True, timeout=3600)
            print(f"Downloaded file: {input_file}")
        except subprocess.TimeoutExpired:
            print(f"Timeout occurred while accessing {s3_file_path}")
            continue
        except subprocess.CalledProcessError as e:
            print(f"Error downloading {svs_file}: {e}")
            continue

        # Define the output file path
        temp_input_file = input_file
        temp_output_file = os.path.join(temp_output_folder, svs_file)

        try:
            # Copy the input file to output folder for processing
            shutil.copy(temp_input_file, temp_output_file)

            # Extract folder name for unique filename generation
            folder_name = os.path.basename(temp_input_folder)

            # Deidentify the file
            deidentify_svs_file(input_file, temp_input_file, temp_output_file, log_file, s3_bucket_input, s3_bucket_output, folder_name)

            # Upload the deidentified file to S3
            try:
                unique_filename = generate_unique_filename(folder_name, os.path.splitext(svs_file)[1])
                s3_output_path = f"{s3_bucket_output}/{unique_filename}"
                subprocess.run(['aws', 's3', 'cp', temp_output_file, s3_output_path], check=True, timeout=3600)
                print(f"Uploaded file to S3: {s3_output_path}")
            except subprocess.TimeoutExpired:
                print(f"Timeout occurred while uploading {svs_file} to {s3_bucket_output}")
                continue
            except subprocess.CalledProcessError as e:
                print(f"Error uploading {svs_file} to S3: {e}")
                continue

        finally:
            # Clean up local files
            if os.path.exists(temp_input_file):
                os.remove(temp_input_file)
                print(f"Deleted local input file: {temp_input_file}")

            if os.path.exists(temp_output_file):
                os.remove(temp_output_file)
                print(f"Deleted local output file: {temp_output_file}")

    # Print transfer stats
    print(f"\nTransfer Stats: Successful Files: {successful_files}, Failed Files: {failed_files}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deidentify SVS files in a directory using user-specified temporary folders')
    parser.add_argument('--input_s3_bucket', required=True, help='Input S3 bucket containing SVS files (e.g., s3://mybucket/path)')
    parser.add_argument('--output_s3_bucket', required=True, help='Output S3 bucket to save deidentified SVS files (e.g., s3://mybucket/output_path)')
    parser.add_argument('--temp_dir', required=True, help='Temporary directory to store intermediate files')
    parser.add_argument('--log_file', required=True, help='Log file path')
    args = parser.parse_args()

    # Process the SVS files one by one
    process_single_svs_file(args.input_s3_bucket, args.output_s3_bucket, args.temp_dir, args.log_file)
