import os
import shutil
import struct
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


def deidentify_svs_files(input_directory, output_directory, start_idx, end_idx):
    """Deidentify a slice of SVS files in the input directory and save them to the output directory with sequential IDs."""
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Get all SVS files in the input directory
    svs_files = [f for f in os.listdir(input_directory) if f.endswith('.svs')]

    # Get the slice of files based on the specified start and end indices
    sliced_files = svs_files[start_idx:end_idx]
    
    for idx, svs_file in enumerate(sliced_files, start=1):
        input_path = os.path.join(input_directory, svs_file)
        
        # Create a new name with a sequential ID for each file
        new_filename = f"{idx:04d}.svs"  # Creates names like 0001.svs, 0002.svs, etc.
        output_path = os.path.join(output_directory, new_filename)

        # Copy the original file to the output directory with the new name
        shutil.copyfile(input_path, output_path)

        # Deidentify the copied file (removing label and macro images)
        print(f"Deidentifying {output_path}...")

        try:
            delete_associated_image(output_path, 'label')
            delete_associated_image(output_path, 'macro')
            print(f"Deidentified {new_filename} successfully.")
        except Exception as e:
            print(f"Failed to deidentify {svs_file}: {e}")


if __name__ == "__main__":
    input_dir = input("Enter the input directory path (with SVS files): ")
    output_dir = input("Enter the output directory path (to save deidentified files): ")

    # List available files
    all_svs_files = [f for f in os.listdir(input_dir) if f.endswith('.svs')]
    
    if not all_svs_files:
        print("No SVS files found in the directory.")
        exit()

    print(f"Found {len(all_svs_files)} SVS files in the input directory.")
    for idx, file in enumerate(all_svs_files):
        print(f"{idx}: {file}")

    # Ask the user to enter the range (start and end index) of files to process
    start_idx = int(input(f"Enter the start index (0 to {len(all_svs_files) - 1}): "))
    end_idx = int(input(f"Enter the end index (1 to {len(all_svs_files)}): "))

    # Process the slice of files
    deidentify_svs_files(input_dir, output_dir, start_idx, end_idx)
    print("Deidentification process completed.")
