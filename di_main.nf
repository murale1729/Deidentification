#!/usr/bin/env nextflow

// Define the parameters
params.input_dir = "/home/path01/bala@path23/bala/data/dombox2"
params.local_dir = "/home/path01/bala@path23/bala/test/temp_folder"
params.output_dir = "/home/path01/bala@path23/bala/test/output_files/deidentified_objects"
params.s3_bucket = "s3://nextflow-bala/Deidentified_Objects/"
params.log_file = "/home/path01/bala@path23/bala/test/log/log.csv"

println "Starting Nextflow Script"

// Count and print the number of files found
def svs_files = file(params.input_dir).listFiles().findAll { it.name.endsWith('.svs') }
println "Files found: ${svs_files.size()}"

// Define the input channel
Channel
    .fromPath("${params.input_dir}/*.svs")
    .into { svs_files_channel }

process deidentifyFilesBatch {

    tag { input_file.name }

    input:
    path input_file from svs_files_channel

    script:
    """
    echo "---------------------------------------------------"
    echo "Processing file: ${input_file}"

    # Extract the folder prefix (directory name)
    folder_prefix=\$(basename \$(dirname "${input_file}"))

    # Define the local file path and output file path
    local_file="${params.local_dir}/\${folder_prefix}_\$(basename "${input_file}")"
    output_file="${params.output_dir}/\${folder_prefix}_DI_\$(basename "${input_file}")"

    # Copy the input file to the local directory
    echo "Copying file to local directory: \${local_file}"
    cp "${input_file}" "\${local_file}" || { echo "Failed to copy file"; exit 1; }

    # Deidentify the file
    echo "Deidentifying file: \${local_file}"
    python3 scripts/deidentify.py --input "\${local_file}" --output "\${output_file}" --log "${params.log_file}" || { echo "Deidentification failed"; exit 1; }

    # Upload to S3
    echo "Uploading deidentified file to S3: ${params.s3_bucket}"
    aws s3 cp "\${output_file}" "${params.s3_bucket}" || { echo "S3 upload failed"; exit 1; }

    # Delete local files
    echo "Deleting local file: \${local_file}"
    rm "\${local_file}" || { echo "Failed to delete local file"; exit 1; }

    echo "Deleting deidentified output file: \${output_file}"
    rm "\${output_file}" || { echo "Failed to delete output file"; exit 1; }

    echo "Finished processing file: ${input_file}"
    echo "---------------------------------------------------"
    """
}

workflow {
    deidentifyFilesBatch()
}

println "Workflow execution finished"
