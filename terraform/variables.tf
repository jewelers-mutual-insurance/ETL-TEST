#Holds shared inputs your modules will use
variable "aws_region"        { 
	type = string 
	default = "us-east-1"
}
variable "artifact_bucket"   { 
	type = string
}
variable "artifact_version"  { 
	type = string 
}
variable "glue_job_role_arn" { 
	type = string 
}
variable "default_output_s3" { 
	type = string 
}


#Deploy only a subset of jobs
variable "deploy_jobs" {
	type = list(string)
	default = []  #empty = deploy all declared jobs
}
