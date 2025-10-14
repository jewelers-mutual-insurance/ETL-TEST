variable "job_name"  {
  type = string 
}
variable "role_arn" { 
  type = string 
}
variable "script_s3_path"  { 
  type = string 
}
variable "glue_version" { 
  type = string 
  default = "4.0" 
}
variable "worker_type" { 
  type = string
  default = "G.1X"
}
variable "number_of_workers" { 
  type = number 
  default = 2  
}
variable "max_retries" { 
  type = number 
  default = 1 
}
variable "timeout" { 
  type = number 
  default = 15 
}
variable "connections" { 
  type = list(string) 
  default = [] 
}
variable "default_arguments" { 
  type = map(string) 
  default = {} 
}
variable "command_name" { 
  type = string 
  default = "glueetl" 
}
