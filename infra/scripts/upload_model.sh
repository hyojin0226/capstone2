#!/usr/bin/env bash
# 모델 산출물(models/<version>/model.json, metadata.json)을 S3 capstone-models/<version>/에 올린다. ADR 0005 결정 1·3
#
# 사용법 (WSL 레포 루트): bash infra/scripts/upload_model.sh models/<version>
#
# - 도구: SeaweedFS 컨테이너 안의 `weed filer.copy` / `weed filer.cat` (새 라이브러리 없음, 2026-11-04 결정)
#   filer 경로 /buckets/<bucket>/… 가 S3 버킷 경로와 같다.
# - 버전 디렉터리는 한 번 쓰면 바꾸지 않는다: 이미 있고 내용(sha256)이 같으면 건너뛰고, 다르면 실패한다.
# - 올린 뒤 다시 읽어서 sha256이 로컬 파일과 같은지 확인한다.

set -euo pipefail

MODEL_DIR=${1:?usage: upload_model.sh models/<version>}
VERSION=$(basename "$MODEL_DIR")
FILER=http://localhost:8888
REMOTE=/buckets/capstone-models/$VERSION
FILES=(model.json metadata.json)

remote_sha() { docker exec seaweedfs sh -c "weed filer.cat $FILER$REMOTE/$1 2>/dev/null | sha256sum | cut -d' ' -f1"; }
EMPTY_SHA=$(printf '' | sha256sum | cut -d' ' -f1)

need_upload=0
for f in "${FILES[@]}"; do
  local_sha=$(sha256sum "$MODEL_DIR/$f" | cut -d' ' -f1)
  r=$(remote_sha "$f")
  if [ "$r" = "$EMPTY_SHA" ]; then need_upload=1
  elif [ "$r" != "$local_sha" ]; then echo "refuse: $REMOTE/$f already exists with different content (versions are immutable)" >&2; exit 1
  fi
done

if [ $need_upload = 1 ]; then
  docker exec seaweedfs mkdir -p "/tmp/upload/$VERSION"
  for f in "${FILES[@]}"; do docker cp "$MODEL_DIR/$f" "seaweedfs:/tmp/upload/$VERSION/$f" >/dev/null; done
  docker exec seaweedfs weed filer.copy "/tmp/upload/$VERSION/model.json" "/tmp/upload/$VERSION/metadata.json" "$FILER$REMOTE/" >/dev/null
  docker exec seaweedfs rm -rf "/tmp/upload/$VERSION"
else
  echo "already uploaded (same sha256): $VERSION"
fi

for f in "${FILES[@]}"; do
  local_sha=$(sha256sum "$MODEL_DIR/$f" | cut -d' ' -f1)
  [ "$(remote_sha "$f")" = "$local_sha" ] || { echo "verify failed: $REMOTE/$f" >&2; exit 1; }
  echo "ok  s3://capstone-models/$VERSION/$f  sha256=${local_sha:0:16}…  $(stat -c %s "$MODEL_DIR/$f") bytes"
done
