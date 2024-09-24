#!/usr/bin/env nextflow

// Define the parameters
params.input_dir = "/home/path01/bala@path23/bala/data/dombox2"
params.local_dir = "/home/path01/bala@path23/bala/test/temp_folder"
params.output_dir = "/home/path01/bala@path23/bala/test/s3_test"
params.log_file = "/home/path01/bala@path23/bala/test/log/log.csv"
params.batch_size = 1 // Default batch size

println "Starting Nextflow Script"

// Define the input channel
Channel.fromPath("${params.input_dir}/*.svs")
       .collate(params.batch_size)
       .set { input_files_channel }

println "Input files collected for processing."

// Process definition
process deidentifyFilesBatch {

    // Enable logging of commands for this process
    echo true  

    input:
    path input_files from input_files_channel

    script:
    """
    echo "Processing files in batch: \$input_files"
    
    for input_file in \${input_files[@]}; do
        echo "---------------------------------------------------"
        echo "Processing file: \$input_file"

        # Extract the folder prefix (directory name)
        folder_prefix=\$(basename \$(dirname "\$input_file"))

        # Define the local file path and output file path
        local_file="${params.local_dir}/\${folder_prefix}_\$(basename "\$input_file")"
        output_file="${params.output_dir}/\${folder_prefix}_DI_\$(basename "\$input_file")"

        # Ensure the output directory exists
        mkdir -p "${params.output_dir}"

        # Copy the input file to the local directory
        echo "Copying file to local directory: \$local_file"
        cp "\$input_file" "\$local_file" || { echo "Failed to copy file"; exit 1; }

        # Deidentify the file
        echo "Deidentifying file: \$local_file"
        python3 /home/path01/bala@path23/bala/Deidentification/deidentification_nf.py --input "\$local_file" --output "\$output_file" --log "${params.log_file}" || { echo "Deidentification failed"; exit 1; }

        # Clean up local file
        echo "Deleting local file: \$local_file"
        rm "\$local_file" || { echo "Failed to delete local file"; exit 1; }

        echo "Finished processing file: \$input_file"
        echo "---------------------------------------------------"
    done
    """
}

workflow {
    deidentifyFilesBatch()
}

println "Workflow execution finished"
