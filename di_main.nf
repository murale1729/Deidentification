#!/usr/bin/env nextflow

// Define the parameters
params.input_dir = "/home/path01/bala@path23/bala/data/dombox2"
params.local_dir = "/home/path01/bala@path23/bala/test/temp_folder"
params.output_dir = "/home/path01/bala@path23/bala/test/output_files/deidentified_objects"
params.s3_bucket = "s3://nextflow-bala/Deidentified_Objects/"
params.log_file = "/home/path01/bala@path23/bala/test/log/log.csv"
params.batch_size = 1 // Default batch size

// Define the process to copy, deidentify, and upload the file
process deidentifyFilesBatch {

    input:
    path input_files from file("${params.input_dir}/*.svs").collect(batchSize: params.batch_size)

    script:
    """
    # Loop through the batch of files
    for input_file in \$input_files; do
        # Extract the folder name from the input file path
        folder_prefix=\$(basename \$(dirname \$input_file))

        # Copy the input file to the local directory with the folder name prefix
        local_file=${params.local_dir}/\${folder_prefix}_\$(basename \$input_file)
        cp \$input_file \$local_file

        # Deidentify the SVS file and save it with the folder name prefix in the output directory
        python3 deidentify_nf.py --input \$local_file --output ${params.output_dir} --log ${params.log_file} --prefix \$folder_prefix

        # Copy the deidentified output file to S3
        aws s3 cp ${params.output_dir}/\${folder_prefix}_DI_\$(basename \$input_file) ${params.s3_bucket}/

        # Delete the local files after copying to S3
        rm \$local_file
        rm ${params.output_dir}/\${folder_prefix}_DI_\$(basename \$input_file)
    done
    """
}

// Workflow definition
workflow {
    deidentifyFilesBatch
}
