# Package

version          = "0.1.1"
author           = "Rirush"
description      = "Downloader of public S3 buckets"
license          = "MIT"
srcDir           = "src"
bin              = @["main"]
namedBin["main"] = "s3dump"


# Dependencies

requires "nim >= 1.6.8"
