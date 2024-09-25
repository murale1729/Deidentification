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

        end_time = time.time()
        time_taken = end_time - start_time

        ext = os.path.splitext(input_file)[1]
        unique_filename = generate_unique_filename(folder_name, ext)

        log_file_update(log_file, os.path.basename(input_file), unique_filename, 'Success', time_taken, input_folder, output_folder)

        successful_files += 1

        completion_percentage = (file_index / total_files) * 100
        print(f"Processed {file_index}/{total_files} files ({completion_percentage:.2f}% complete)")

    except Exception as e:
        print(f"Failed to deidentify {input_file}: {e}")
        end_time = time.time()
        time_taken = end_time - start_time
        log_file_update(log_file, os.path.basename(input_file), os.path.basename(temp_output_file), f'Failed: {e}', time_taken, input_folder, output_folder)
        failed_files += 1

def process_svs_files(input_dir, output_dir, temp_dir, log_file):
    """Process all SVS files in the input directory using user-specified temporary folders."""
    global total_files

    total_files = sum(len(files) for _, _, files in os.walk(input_dir) if any(f.lower().endswith('.svs') for f in files))

    if total_files == 0:
        print("No SVS files found.")
        return

    temp_input_folder = os.path.join(temp_dir, 'temp_input_folder')
    temp_output_folder = os.path.join(temp_dir, 'temp_output_folder')

    os.makedirs(temp_input_folder, exist_ok=True)
    os.makedirs(temp_output_folder, exist_ok=True)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

    file_index = 1

    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.lower().endswith('.svs'):
                input_file = os.path.join(root, filename)

                temp_input_file = os.path.join(temp_input_folder, filename)
                temp_output_file = os.path.join(temp_output_folder, filename)

                print(f"\nProcessing file: {input_file}")

                folder_name = os.path.basename(root)

                deidentify_svs_file(input_file, temp_input_file, temp_output_file, log_file, root, output_dir, folder_name, file_index, total_files)

                relative_path = os.path.relpath(root, input_dir)
                output_subdir = os.path.join(output_dir, relative_path)
                if not os.path.exists(output_subdir):
                    os.makedirs(output_subdir)

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

    print(f"\nTransfer Stats: Successful Files: {successful_files}, Failed Files: {failed_files}")

def sync_from_s3(input_s3_bucket, local_input_dir):
    """Sync files from the input S3 bucket to the local directory."""
    try:
        print(f"Syncing files from S3 bucket {input_s3_bucket} to {local_input_dir}")
        subprocess.run(['aws', 's3', 'sync', input_s3_bucket, local_input_dir], check=True)
        print(f"Successfully synced from {input_s3_bucket}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to sync from S3 bucket {input_s3_bucket}: {e}")

def sync_to_s3(output_dir, s3_bucket):
    """Sync local output directory to S3 bucket."""
    try:
        print(f"Syncing {output_dir} to S3 bucket {s3_bucket}")
        subprocess.run(['aws', 's3', 'sync', output_dir, s3_bucket], check=True)
        print(f"Successfully synced {output_dir} to {s3_bucket}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to sync {output_dir} to S3: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deidentify SVS files in a directory using user-specified temporary folders')
    parser.add_argument('--input_s3_bucket', required=True, help='Input S3 bucket containing SVS files')
    parser.add_argument('--output_s3_bucket', required=True, help='Output S3 bucket to save deidentified SVS files')
    parser.add_argument('--temp_dir', required=True, help='Temporary directory to store intermediate files')
    parser.add_argument('--log_file', required=True, help='Log file path')
    args = parser.parse_args()

    local_input_dir = os.path.join(args.temp_dir, 'local_input')
    local_output_dir = os.path.join(args.temp_dir, 'local_output')

    # Sync files from S3 to the local temp input directory
    sync_from_s3(args.input_s3_bucket, local_input_dir)

    # Process the files locally
    process_svs_files(local_input_dir, local_output_dir, args.temp_dir, args.log_file)

    # Sync the output files to the S3 bucket
    sync_to_s3(local_output_dir, args.output_s3_bucket)
