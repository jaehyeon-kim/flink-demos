#!/bin/bash -ex

## Allocate elastic IP and disable source/destination checks
TOKEN=$(curl --silent --max-time 60 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 30")
INSTANCEID=$(curl --silent --max-time 60 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
aws --region ${aws_region} ec2 associate-address --instance-id $INSTANCEID --allocation-id ${allocation_id}
aws --region ${aws_region} ec2 modify-instance-attribute --instance-id $INSTANCEID --source-dest-check "{\"Value\": false}"

## Start SoftEther VPN server
yum update -y && yum install docker -y
systemctl enable docker.service && systemctl start docker.service

docker pull siomiz/softethervpn:debian
docker run -d \
  --cap-add NET_ADMIN \
  --name softethervpn \
  --restart unless-stopped \
  -p 500:500/udp -p 4500:4500/udp -p 1701:1701/tcp -p 1194:1194/udp -p 5555:5555/tcp -p 443:443/tcp \
  -e PSK=${vpn_psk} \
  -e SPW=${admin_password} \
  -e HPW=DEFAULT \
  siomiz/softethervpn:debian