#!/bin/bash
# Quick throwaway EC2 instance for benchmarking in us-east-1
# Usage:
#   ./ec2-bench.sh launch    # create instance, print SSH command
#   ./ec2-bench.sh ssh       # SSH into it
#   ./ec2-bench.sh kill      # terminate and clean up

set -e
REGION="us-east-1"
KEY_NAME="bench-key"
KEY_FILE="bench-key.pem"
SG_NAME="bench-sg"
INSTANCE_TYPE="c5.xlarge"
STATE_FILE=".ec2-bench-instance"

case "${1:-}" in
launch)
    # Create key pair (skip if exists)
    if [ ! -f "$KEY_FILE" ]; then
        echo "Creating key pair..."
        aws ec2 create-key-pair --region $REGION --key-name $KEY_NAME \
            --query 'KeyMaterial' --output text > $KEY_FILE
        chmod 600 $KEY_FILE
    fi

    # Create security group with SSH access (skip if exists)
    SG_ID=$(aws ec2 describe-security-groups --region $REGION \
        --filters "Name=group-name,Values=$SG_NAME" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")

    if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
        echo "Creating security group with SSH access..."
        SG_ID=$(aws ec2 create-security-group --region $REGION \
            --group-name $SG_NAME --description "Benchmark SSH access" \
            --query 'GroupId' --output text)
        aws ec2 authorize-security-group-ingress --region $REGION \
            --group-id $SG_ID --protocol tcp --port 22 --cidr 0.0.0.0/0
    fi
    echo "Security group: $SG_ID"

    # Launch instance
    echo "Launching $INSTANCE_TYPE in $REGION..."
    INSTANCE_ID=$(aws ec2 run-instances --region $REGION \
        --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
        --instance-type $INSTANCE_TYPE \
        --key-name $KEY_NAME \
        --security-group-ids $SG_ID \
        --associate-public-ip-address \
        --query 'Instances[0].InstanceId' --output text)

    echo $INSTANCE_ID > $STATE_FILE
    echo "Instance: $INSTANCE_ID"
    echo "Waiting for instance to start..."
    aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_ID

    IP=$(aws ec2 describe-instances --region $REGION --instance-ids $INSTANCE_ID \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

    echo ""
    echo "Ready! Connect with:"
    echo "  ./ec2-bench.sh ssh"
    echo ""
    echo "Then inside the instance:"
    echo "  sudo yum install -y git gcc"
    echo "  git clone https://github.com/WalBeh/inserter.git && cd inserter"
    echo "  echo 'CRATE_CONNECTION_STRING=https://admin:PASSWORD@xdemo2.eks1.us-east-1.aws.cratedb-dev.net:4200' > .env"
    echo "  cp .env rust/.env"
    echo "  # Python"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh && source \$HOME/.local/bin/env"
    echo "  uv venv && source .venv/bin/activate && uv pip install -e ."
    echo "  uv run crate-write --benchmark --table-name bench_ec2 --duration 2 --threads 64 --batch-size 1200 --batch-interval 0 --shards 12"
    echo "  # Rust"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && source \$HOME/.cargo/env"
    echo "  cd rust && cargo run --release -- --benchmark --table-name bench_ec2_rs --duration 2 --threads 128 --batch-size 1000 --batch-interval 0 --shards 12"
    echo ""
    echo "When done: ./ec2-bench.sh kill"
    ;;

ssh)
    if [ ! -f "$STATE_FILE" ]; then
        echo "No instance found. Run: ./ec2-bench.sh launch"
        exit 1
    fi
    INSTANCE_ID=$(cat $STATE_FILE)
    IP=$(aws ec2 describe-instances --region $REGION --instance-ids $INSTANCE_ID \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    echo "Connecting to $IP..."
    ssh -i $KEY_FILE -o StrictHostKeyChecking=no ec2-user@$IP
    ;;

kill)
    if [ ! -f "$STATE_FILE" ]; then
        echo "No instance found."
        exit 0
    fi
    INSTANCE_ID=$(cat $STATE_FILE)
    echo "Terminating $INSTANCE_ID..."
    aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID > /dev/null
    rm -f $STATE_FILE
    echo "Done. Instance will be terminated shortly."
    echo "Note: key pair '$KEY_NAME' and security group '$SG_NAME' left in AWS for reuse."
    echo "To fully clean up:"
    echo "  aws ec2 delete-key-pair --region $REGION --key-name $KEY_NAME"
    echo "  aws ec2 delete-security-group --region $REGION --group-name $SG_NAME"
    echo "  rm -f $KEY_FILE"
    ;;

*)
    echo "Usage: $0 {launch|ssh|kill}"
    exit 1
    ;;
esac
