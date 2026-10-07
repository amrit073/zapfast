"""Prepare the existing binary recipe for a locally built x86_64 archive."""

import hashlib
from pathlib import Path
import shutil
import sys

version, archive_path = sys.argv[1:]
archive = Path(archive_path)
out = archive.parent / "arch"
out.mkdir()
shutil.copy2(archive, out / archive.name)
shutil.copy2("packaging/arch/zapfast.install", out / "zapfast-bin.install")
recipe = Path("packaging/arch/zapfast-bin/PKGBUILD.in").read_text()
recipe = recipe.replace("@VERSION@", version).replace("@PKGREL@", "1")
recipe = recipe.replace("@AMD64_SHA256@", hashlib.sha256(archive.read_bytes()).hexdigest())
recipe = recipe.replace("arch=('x86_64' 'aarch64')", "arch=('x86_64')")
recipe = recipe.replace(
    '${_repo}/releases/download/v${pkgver}/zapfast-v${pkgver}-x86_64-unknown-linux-gnu.tar.gz',
    archive.name,
)
recipe = "\n".join(
    line for line in recipe.splitlines()
    if not line.startswith(("source_aarch64=", "sha256sums_aarch64=", "_repo="))
) + "\n"
(out / "PKGBUILD").write_text(recipe)
