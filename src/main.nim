import s3, strutils, os, atomics

var
  prefix: string
  bucket: string
  target: string

type ThreadConfig = object
  prefix: string
  bucket: string
  target: string

var
  chan: Channel[Object]
  thr: array[0..7, Thread[ThreadConfig]]
  c: Atomic[int]

proc fileDownloader(cfg: ThreadConfig) {.gcSafe.} =
  while true:
    let obj = chan.recv

    var fsName = obj.key
    removePrefix(fsName, cfg.prefix)
    fsName = cfg.target & fsName
    echo "Downloading ", obj.key, " to ", fsName
    createDir splitPath(fsName).head
    downloadObject(cfg.bucket, obj.key, fsName)

    c.atomicInc 1

when isMainModule:
  let params = commandLineParams()
  if len(params) != 3:
    quit("usage: " & getAppFilename() & " bucket prefix target")

  bucket = params[0]
  prefix = params[1]
  target = params[2]

  chan.open()

  for i in 0..high(thr):
    createThread(thr[i], fileDownloader, ThreadConfig(prefix: prefix, bucket: bucket, target: target))

  echo "Downloading bucket contents..."

  var o = listObjects(bucket,
      prefix = prefix)

  for obj in o.contents:
    chan.send obj

  while c.load != o.contents.len:
    sleep 1000

  c.store 0

  while o.truncated:
    o = listObjects(bucket,
      prefix = prefix)

    for obj in o.contents:
      chan.send obj

    while c.load != o.contents.len:
      sleep 1000

    c.store 0

    echo "Done"
