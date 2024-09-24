#!/usr/bin/env nextflow

// Define the parameters
params.input_dir = "/home/path01/bala@path23/bala/data/dombox2"
params.local_dir = "/home/path01/bala@path23/bala/test/temp_folder"
params.output_dir = "/home/path01/bala@path23/bala/test/s3_test"
params.log_file = "/home/path01/bala@path23/bala/test/log/log.csv"

println "Starting Nextflow Script"
println "Input directory: ${params.input_dir}"
println "Local directory: ${params.local_dir}"
println "Output directory: ${params.output_dir}"
println "Log file path: ${params.log_file}"

// Define the input channel
def input_files_channel = Channel.fromPath("${params.input_dir}/*.svs")

println "Input files collected for processing: ${params.input_dir}"

// Process definition
process deidentifyFiles {
    
    // Enable debugging of commands for this process
    debug true  
    println "Starting deidentifyFiles"
    
    input:
        path input_file

    script:
    """
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
    python3 /home/path01/bala@path23/bala/Deidentification/deidentification_nf.py --input "\$local_file" --output "\$output_file" --log "${params.log_file}"
    
    # Check if the deidentification script succeeded
    if [ \$? -ne 0 ]; then
        echo "Deidentification failed for file: \$input_file"
        failed_file="\$input_file"
        echo "FAILED: \$failed_file" >> "${params.log_file}"
    else
        echo "Deidentification succeeded for file: \$input_file"
    fi

    # Clean up local file
    echo "Deleting local file: \$local_file"
    rm "\$local_file" || { echo "Failed to delete local file"; exit 1; }

    echo "Finished processing file: \$input_file"
    echo "---------------------------------------------------"
    """
    println "Finished processing file: ${input_file}"
}

workflow {
    // Create the input channel
    input_files_channel = Channel.fromPath("${params.input_dir}/*.svs")

    // Pass the input channel to the process
    deidentifyFiles(input_files_channel)
    println "Workflow execution finished"
    println "Results saved to: ${params.output_dir}"
}
