data "template_file" "ec2_userdata" {
  template = "${file("${path.cwd}/userdata.tpl")}"
}

resource "aws_instance" "web" {  
  # timescaledb ami in us-east-1
  ami           = "ami-00fd91eb722f59b02" 
  instance_type = "t2.micro"

  user_data = data.template_file.ec2_userdata.template

  tags = {
    Name = "example-co"
  }
}