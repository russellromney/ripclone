# Delete the load-test prefixes listed in /root/old_prefixes.txt plus finished runs' prefixes. Only loadtest-* prefixes.
set -a; . /root/s3.env; set +a
for r in "$@"; do cat /data/run/$r/s3_prefix >> /root/old_prefixes.txt; done
sort -u /root/old_prefixes.txt | while read p; do
  case "$p" in loadtest-*/) ;; *) echo "SKIP $p"; continue;; esac
  aws s3 rm --recursive --quiet "s3://$RIPCLONE_S3_BUCKET/$p" && echo "DELETED $p"
  aws s3 ls --recursive --summarize "s3://$RIPCLONE_S3_BUCKET/$p" | grep 'Total Objects'
done
