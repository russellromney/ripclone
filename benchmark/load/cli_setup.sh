# client box: git + a dir for probe clones. ripclone binary and token are copied in by scp.
set -eux
export DEBIAN_FRONTEND=noninteractive
apt-get update -q && apt-get install -y -q git python3
mkdir -p /data /opt/rc/bin
echo CLI_SETUP_DONE
