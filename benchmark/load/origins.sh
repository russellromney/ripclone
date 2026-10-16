# Bare-clone every repo in repos.txt from GitHub into /opt/rc/origin, 3 at a time.
set -u
mkdir -p /opt/rc/origin
mk() { local r=$1 d=/opt/rc/origin/$1.git s=$(date +%s)
  mkdir -p $(dirname $d)
  if git clone --bare --quiet https://github.com/$r.git $d; then
    git -C $d config uploadpack.allowAnySHA1InWant true
    git -C $d config uploadpack.allowReachableSHA1InWant true
    echo "ORIGIN_OK $r $(( $(date +%s) - s ))s $(du -sh $d | cut -f1)"
  else echo "ORIGIN_FAIL $r"; fi; }
grep -v '^#' /root/repos.txt | awk 'NF{print $1}' | xargs -P 3 -I{} bash -c "$(declare -f mk); mk {}"
echo ORIGINS_DONE
