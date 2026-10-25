#!/usr/bin/env bash
# Build the cascade bundle: a self-contained /opt/cascade that runs `cascade node wrap`
# inside any Linux image, including ones with no Python, a different libc, or nothing at
# all (FROM scratch).
#
# What makes it self-contained: a relocatable CPython built against musl
# (python-build-standalone), shipped together with musl's dynamic loader, which for musl
# is the whole libc. The interpreter's ELF interpreter is pointed at that loader, so
# nothing is taken from the image. LD_LIBRARY_PATH is never set, so the command `wrap`
# starts sees only its own image's libraries.
#
# Output (under dist/bundle/, git-ignored):
#   opt-cascade/                                 unpacked, for mounting at /opt/cascade
#   cascade-bundle-<version>-x86_64.tar.gz       reproducible archive of the same tree
#   cascade-bundle-<version>-x86_64.tar.gz.sha256
#
# Entry point inside a container:  /opt/cascade/bin/cascade node wrap -- <command...>
#
# Needs: docker, poetry, curl, sha256sum, strip and readelf (binutils). x86_64 only for now.
set -euo pipefail

PBS_TAG=20261021
PY_VERSION=3.13.17
PBS_ASSET="cpython-${PY_VERSION}+${PBS_TAG}-x86_64-unknown-linux-musl-install_only.tar.gz"
PBS_SHA256=385b6f7e969192ef67993c3223eaff80359154026627e9c1bbeb9af016b5f4cb
# source of the musl loader and of patchelf; pinned, so the loader is reproducible
ALPINE=alpine:3.22@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f

REPO="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$REPO/dist/bundle"
CACHE="$OUT/cache"      # downloads only; nothing is executed from here
WORK="$OUT/work"
ROOT="$WORK/opt-cascade"
SITE="$ROOT/lib/python3.13/site-packages"

for tool in docker poetry curl sha256sum strip readelf; do
  command -v "$tool" >/dev/null || { echo "build.sh: $tool is required" >&2; exit 1; }
done
VERSION="$(poetry -C "$REPO" version -s)"
ARCHIVE="$OUT/cascade-bundle-${VERSION}-x86_64.tar.gz"

mkdir -p "$CACHE"
# files written by containers below are chowned back, but clear any root-owned leftovers
docker run --rm -v "$OUT:/out" "$ALPINE" rm -rf /out/work /out/opt-cascade
mkdir -p "$WORK"

# 1. the interpreter, verified against the release's published checksum
if [ ! -f "$CACHE/$PBS_ASSET" ]; then
  curl -sSfL -o "$CACHE/$PBS_ASSET.part" \
    "https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/${PBS_ASSET}"
  mv "$CACHE/$PBS_ASSET.part" "$CACHE/$PBS_ASSET"
fi
echo "$PBS_SHA256  $CACHE/$PBS_ASSET" | sha256sum -c --quiet
tar -xzf "$CACHE/$PBS_ASSET" -C "$WORK"
mv "$WORK/python" "$ROOT"

# 2. cascade and its dependencies, at the versions in poetry.lock, as musllinux wheels.
#    Installed from this host with --target/--platform: the bundle's interpreter cannot
#    run here until its loader is patched in.
rm -rf "$CACHE/dist"
(cd "$REPO" && poetry build -q -f wheel -o "$CACHE/dist")
poetry -C "$REPO" show --only main | awk '{print $1 "==" $2}' > "$CACHE/constraints.txt"
poetry -C "$REPO" run pip install --quiet --disable-pip-version-check --no-compile \
  --target "$SITE" --python-version 3.13 --implementation cp --only-binary=:all: \
  --platform musllinux_1_2_x86_64 -c "$CACHE/constraints.txt" \
  "$CACHE"/dist/cascade_pdl-*.whl
rm -rf "$SITE/bin"   # console scripts would point at this host's interpreter

# 3. the entry point. The kernel reads the shebang, so no shell is needed in the image.
cat > "$ROOT/bin/cascade" <<'EOF'
#!/opt/cascade/bin/python3 -I
import sys
from cascade.cli import main
sys.exit(main())
EOF
chmod 755 "$ROOT/bin/cascade"

# 4. drop what a runtime does not need
rm -rf "$ROOT/include" "$ROOT/share" "$ROOT"/lib/pkgconfig "$ROOT"/lib/python3.13/config-3.13-* \
       "$ROOT"/lib/python3.13/{test,idlelib,tkinter,turtledemo,ensurepip} \
       "$ROOT"/lib/python3.13/lib-dynload/_tkinter* \
       "$ROOT"/lib/libtcl* "$ROOT"/lib/libtk* "$ROOT"/lib/tcl* "$ROOT"/lib/tk* \
       "$ROOT"/lib/itcl* "$ROOT"/lib/thread* \
       "$ROOT"/bin/{idle3*,pydoc3*,pip*,python3*-config}
rm -rf "$ROOT"/lib/python3.13/site-packages/pip*
# the interpreter needs libpython.so only when it links it dynamically
if ! readelf -d "$ROOT/bin/python3.13" | grep -q 'NEEDED.*libpython'; then
  rm -f "$ROOT"/lib/libpython3.13.so*
fi
# botocore ships every AWS service's model; the node side talks to S3, and the credential
# providers need sts / sso / sso-oidc
find "$SITE/botocore/data" -mindepth 1 -maxdepth 1 -type d \
  ! -name s3 ! -name sts ! -name sso ! -name sso-oidc -exec rm -rf {} +
# debug info is most of the size (the musl interpreter strips safely)
find "$ROOT" -type f -name '*.so*' -exec strip --strip-debug {} + 2>/dev/null || true

# 5. in a pinned alpine: add the loader, point the interpreter at it, precompile.
#    Bytecode uses unchecked-hash pycs: no source mtimes inside (reproducible), and the
#    bundle is mounted read-only, so it could not write a cache at run time anyway.
docker run --rm -v "$ROOT:/opt/cascade" "$ALPINE" sh -euc "
  apk add -q --no-cache patchelf >/dev/null
  cp /lib/ld-musl-x86_64.so.1 /opt/cascade/lib/
  patchelf --set-interpreter /opt/cascade/lib/ld-musl-x86_64.so.1 /opt/cascade/bin/python3.13
  /opt/cascade/bin/python3 -I -m compileall -q -j0 --invalidation-mode unchecked-hash \
    /opt/cascade/lib/python3.13 >/dev/null
  chown -R $(id -u):$(id -g) /opt/cascade"
strip --strip-debug "$ROOT/bin/python3.13"

# 6. smoke test: the bundle alone, in an image with nothing in it
docker run --rm --network none -v "$ROOT:/opt/cascade:ro" --entrypoint /opt/cascade/bin/cascade \
  "$ALPINE" node wrap --help >/dev/null

# 7. publish: the unpacked tree and a reproducible archive of it
mv "$ROOT" "$OUT/opt-cascade"
rmdir "$WORK"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -C "$OUT" -cf - opt-cascade \
  | gzip -n > "$ARCHIVE"
(cd "$OUT" && sha256sum "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE").sha256")

echo "bundle: $OUT/opt-cascade ($(du -sh "$OUT/opt-cascade" | cut -f1))"
echo "archive: $ARCHIVE ($(du -h "$ARCHIVE" | cut -f1))"
echo "sha256: $(cut -d' ' -f1 "$ARCHIVE.sha256")"
