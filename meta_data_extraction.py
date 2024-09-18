import openslide
from PIL import Image

# Path to the SVS file
svs_path = "images/CMU-1.svs"

# Open the whole-slide image using OpenSlide
slide = openslide.OpenSlide(svs_path)

# Check if the file contains label and macro images
if 'label' in slide.associated_images:
    # Extract the label image
    label_image = slide.associated_images['label']
    label_image.show()  # Display the label image in the notebook

if 'macro' in slide.associated_images:
    # Extract the macro image (low-res overview of the entire slide)
    macro_image = slide.associated_images['macro']
    macro_image.show()  # Display the macro image in the notebook

# You can also save the label and macro images if needed
label_image.save("label_images/label_image.png")
macro_image.save("output_images/macro_image.png")