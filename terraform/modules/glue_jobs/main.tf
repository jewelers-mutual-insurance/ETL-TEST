resource "aws_glue_job" "this" {
	name     = var.job_name
	role_arn = var.role.arn

	command {
	name = var.command_name
	script_location = var.script_s3_path
	python_version = "3"
}

	glue_version = var.glue_version
	worker_type = var.worker_type
	number_of_workers = var.number_of_workers
	max_retries = var.max_retries
	timeout = var.timeout
	connections = var.connections
	default_arguments = var.default_arguments

}

output "job_name" { value = aws_glue_job.this.name }
