#!/usr/bin/env nextflow

// Define the parameters
params.input_dir = "/home/path01/bala@path23/bala/data/dombox2"
params.local_dir = "/home/path01/bala@path23/bala/test/temp_folder"
params.output_dir = "/home/path01/bala@path23/bala/test/output_files/deidentified_objects"
params.s3_bucket = "s3://nextflow-bala/Deidentified_Objects/"
params.log_file = "/home/path01/bala@path23/bala/test/log/log.csv"
params.batch_size = 1 // Default batch size

// Debug input files collection
def input_files_list = file("${params.input_dir}/*.svs").collect()
println "Input files collected for processing: ${input_files_list}"

process deidentifyFilesBatch {

    echo true  // Enable logging for this process

    input:
    path input_files from input_files_list.collect(batchSize: params.batch_size)

    script:
    """
    echo "Processing files in batch: \$input_files"
    
    for input_file in \$input_files; do
        echo "---------------------------------------------------"
        echo "Processing file: \$input_file"

        # Extract the folder prefix (directory name)
        folder_prefix=\$(basename \$(dirname \$input_file))

        # Define the local file path and output file path
        local_file=${params.local_dir}/\${folder_prefix}_\$(basename \$input_file)
        output_file=${params.output_dir}/\${folder_prefix}_DI_\$(basename \$input_file)

        # Copy the input file to the local directory
        echo "cp \$input_file \$local_file"
        cp \$input_file \$local_file || { echo "Failed to copy file"; exit 1; }

        # Deidentify the file
        echo "Running deidentify.py with --input \$local_file --output \$output_file"
        python3 /home/path01/bala@path23/bala/Deidentification/deidentification_nf.py --input \$local_file --output \$output_file --log ${params.log_file} || { echo "Deidentification failed"; exit 1; }

        # Upload the deidentified file to S3
        echo "Uploading file to S3: aws s3 cp \$output_file ${params.s3_bucket}"
        aws s3 cp \$output_file ${params.s3_bucket} || { echo "S3 upload failed"; exit 1; }

        # Clean up local files
        echo "Deleting local file: \$local_file"
        rm \$local_file || { echo "Failed to delete local file"; exit 1; }

        echo "Deleting deidentified output file: \$output_file"
        rm \$output_file || { echo "Failed to delete output file"; exit 1; }

        echo "Finished processing file: \$input_file"
        echo "---------------------------------------------------"
    done
    """
}

workflow {
    deidentifyFilesBatch
}

println "Workflow execution finished"
