import openslide
from PIL import Image
import os

def list_svs_files(directory):
    """List all SVS files in the directory."""
    svs_files = [f for f in os.listdir(directory) if f.endswith(".svs")]
    return svs_files

def process_svs_files(file_list, svs_directory, output_label_dir, output_macro_dir):
    """Process a subset of SVS files, extracting and saving label and macro images."""
    for svs_file in file_list:
        file_path = os.path.join(svs_directory, svs_file)
        try:
            slide = openslide.OpenSlide(file_path)
        except openslide.OpenSlideError as e:
            print(f"Error opening slide {svs_file}: {e}")
            continue

        # Check for and save the label image
        if 'label' in slide.associated_images:
            label_image = slide.associated_images['label']
            label_image.save(os.path.join(output_label_dir, f"label_{svs_file}.png"))
            print(f"Label image for {svs_file} saved as label_{svs_file}.png")
        else:
            print(f"No label image found for {svs_file}")

        # Check for and save the macro image
        if 'macro' in slide.associated_images:
            macro_image = slide.associated_images['macro']
            macro_image.save(os.path.join(output_macro_dir, f"macro_{svs_file}.png"))
            print(f"Macro image for {svs_file} saved as macro_{svs_file}.png")
        else:
            print(f"No macro image found for {svs_file}")


def main():
    # Define paths
    svs_directory = "/home/ubuntu/mntdr/dombox1/dombox2/"
    label_output_dir = "/home/ubuntu/mntdr/dombox1/test/output/label_images/"
    macro_output_dir = "/home/ubuntu/mntdr/dombox1/test/output/macro_images/"

    # Ensure output directories exist
    os.makedirs(label_output_dir, exist_ok=True)
    os.makedirs(macro_output_dir, exist_ok=True)

    # Get all SVS files in the directory
    all_svs_files = list_svs_files(svs_directory)

    # subset of files to process 
    subset_files = all_svs_files[:10]  

    print(f"Processing {len(subset_files)} file(s): {subset_files}")

    # Process the subset
    process_svs_files(subset_files, svs_directory, label_output_dir, macro_output_dir)


if __name__ == "__main__":
    main()
