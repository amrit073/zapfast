Personal x86_64 Arch Linux build of ZapFast from this fork.

Download the `.pkg.tar.zst` and `checksums.txt` below into the same folder, then run:

```sh
sha256sum --check checksums.txt
sudo pacman -U ./zapfast-bin-*.pkg.tar.zst
```

Launch **ZapFast** from your application menu or run `zapfast`.

The package was installed and checked in a disposable Arch Linux container.
GUI and live calls still require testing on a desktop. `BUILD.txt` identifies
the source commit and workflow run. This is a personal development build.
