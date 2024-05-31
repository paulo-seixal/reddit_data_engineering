terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "5.51.1"
    }
  }
}

# Configure AWS provider
provider "aws" {
    region = var.aws_region
}

# Configure redshift 
resource "aws_redshift_cluster" "redshift" {
  cluster_identifier = "redshift-cluster-pipeline"
  skip_final_snapshot = true
  master_username    = "awsuser"
  master_password    = var.db_password
  node_type          = "dc2.large"
  cluster_type       = "single-node"
  publicly_accessible = "true"
  iam_roles = [aws_iam_role.redshift_role.arn]
  vpc_security_group_ids = [aws_security_group.sg_redshift.id]
  
}

# Confuge security group for Redshift allowing all inbound/outbound traffic
 resource "aws_security_group" "sg_redshift" {
  name        = "sg_redshift"
  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
  }
  egress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
  }
}

# Create S3 Read only access role.
resource "aws_iam_role" "redshift_role" {
  name = "RedShiftLoadRole"
  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"]
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      },
    ]
  })
}

# Create S3 bucket
resource "aws_s3_bucket" "reddit_bucket" {
  bucket = var.s3_bucket
  force_destroy = true # will delete contents of bucket when we run terraform destroy
}


      