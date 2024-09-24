#!/usr/bin/env nextflow

// Define the parameters
params.input_dir = "/home/path01/bala@path23/bala/data/dombox2"
params.local_dir = "/home/path01/bala@path23/bala/test/temp_folder"
params.output_dir = "/home/path01/bala@path23/bala/test/output_files/deidentified_objects"
params.s3_bucket = "s3://nextflow-bala/Deidentified_Objects/"
params.log_file = "/home/path01/bala@path23/bala/test/log/log.csv"
params.batch_size = 1 // Default batch size

process deidentifyFilesBatch {

    input:
    path input_files from file("${params.input_dir}/*.svs").collect(batchSize: params.batch_size)

    script:
    """
    echo "Processing batch of files: \$input_files"

    for input_file in \$input_files; do
        echo "---------------------------------------------------"
        echo "Processing file: \$input_file"
        
        # Extract the folder prefix (directory name)
        folder_prefix=\$(basename \$(dirname \$input_file))
        
        # Define the local file path and output file path
        local_file=${params.local_dir}/\${folder_prefix}_\$(basename \$input_file)
        output_file=${params.output_dir}/\${folder_prefix}_DI_\$(basename \$input_file)

        # Copy the input file to the local directory
        echo "Copying file to local directory: \$local_file"
        cp \$input_file \$local_file

        # Print stage before deidentifying the file
        echo "Deidentifying file: \$local_file"
        python3 scripts/deidentify.py --input \$local_file --output \$output_file --log ${params.log_file}
        
        # Print stage before uploading to S3
        echo "Uploading deidentified file to S3: ${params.s3_bucket}/\$(basename \$output_file)"
        aws s3 cp \$output_file ${params.s3_bucket}/

        # Print stage before deleting local files
        echo "Deleting local file: \$local_file"
        rm \$local_file

        echo "Deleting deidentified output file: \$output_file"
        rm \$output_file

        echo "Finished processing file: \$input_file"
        echo "---------------------------------------------------"
    done
    """
}

workflow {
    deidentifyFilesBatch
}
