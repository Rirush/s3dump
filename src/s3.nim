import times, httpclient, xmlparser, xmltree, parseutils, uri, tables, sequtils, strutils

type
    Owner* = object
        displayName*: string
        id*: string

    Object* = object
        checksumAlgorithm*: seq[string]
        etag*: string
        key*: string
        lastModified*: DateTime
        owner*: Owner
        size*: uint
        storageClass*: string

    ListResponse* = object
        truncated*: bool
        contents*: seq[Object]
        name*: string
        prefix*: string
        delimiter*: string
        maxKeys*: uint
        commonPrefixes*: seq[string]
        encodingType*: string
        keyCount*: uint
        continuationToken*: string
        nextContinuationToken*: string
        startAfter*: string

    FailedRequestException* = ref object of CatchableError
        code: HttpCode

proc parseBool(t: string): bool =
    assert t == "true" or t == "false" or t == ""
    return t == "true"

proc extractText(n: XmlNode): string =
    if n.len == 0:
        ""
    else:
        map(n.items.toSeq, proc(x: XmlNode): string = x.text).join("")

proc parseListResponse(n: XmlNode): ListResponse =
    assert n.tag == "ListBucketResult"

    for el in n.items:
        if el.kind == xnElement:
            case el.tag:
                of "IsTruncated":
                    result.truncated = el.extractText.parseBool

                of "Name":
                    result.name = el.extractText

                of "Prefix":
                    result.prefix = el.extractText

                of "Delimiter":
                    result.delimiter = el.extractText

                of "MaxKeys":
                    try:
                        discard parseUInt(el.extractText, result.maxKeys)
                    except:
                        discard

                of "EncodingType":
                    result.encodingType = el.extractText

                of "KeyCount":
                    try:
                        discard parseUInt(el.extractText, result.keyCount)
                    except:
                        discard

                of "ContinuationToken":
                    result.continuationToken = el.extractText

                of "NextContinuationToken":
                    result.nextContinuationToken = el.extractText

                of "StartAfter":
                    result.startAfter = el.extractText

                of "Contents":
                    var c = Object()

                    for el in el.items:
                        case el.tag:
                            of "ChecksumAlgorithm":
                                c.checksumAlgorithm.add(el.extractText)

                            of "ETag":
                                c.etag = el.extractText

                            of "Key":
                                c.key = el.extractText

                            of "LastModified":
                                c.lastModified = el.extractText.parse("YYYY-MM-dd'T'HH:mm:ss'.'fff'Z'")

                            of "Owner":
                                for el in el.items:
                                    case el.tag:
                                        of "DisplayName":
                                            c.owner.displayName = el.extractText
                                        of "ID":
                                            c.owner.id = el.extractText

                            of "Size":
                                try:
                                    discard parseUInt(el.extractText, c.size)
                                except:
                                    discard

                            of "StorageClass":
                                c.storageClass = el.extractText

                    result.contents.add(c)

                of "CommonPrefixes":
                    for el in el.items:
                        case el.tag:
                            of "Prefix":
                                result.commonPrefixes.add(el.extractText)

proc `$`*(exp: FailedRequestException): string = "AWS request failed: " & $exp.code

proc listObjects*(bucket: string, continuationToken = "", delimiter = "",
        encodingType = "", fetchOwner = false, maxKeys = 1000, prefix = "",
        startAfter = ""): ListResponse =
    let client = newHttpClient(timeout = 10_000)
    defer: client.close()
    var params = {"list-type": "2", "max-keys": $maxKeys}.toTable
    var uri = parseUri("https://" & bucket & ".s3.amazonaws.com/")

    if continuationToken != "":
        params["continuation-token"] = continuationToken
    if delimiter != "":
        params["delimiter"] = delimiter
    if encodingType != "":
        params["encoding-type"] = encodingType
    if fetchOwner:
        params["fetch-owner"] = "true"
    if prefix != "":
        params["prefix"] = prefix
    if startAfter != "":
        params["start-after"] = startAfter

    let resp = client.get(uri ? params.pairs.toSeq)
    if resp.code != HttpCode(200):
        raise FailedRequestException(msg: "AWS request failed: " & $resp.code,
                code: resp.code)

    let tree = resp.bodyStream.parseXml({})

    return tree.parseListResponse

proc downloadObject*(bucket: string, key: string, path: string) =
    let client = newHttpClient()
    defer: client.close()
    let url = parseUri("https://" & bucket & ".s3.amazonaws.com/") / key
    client.downloadFile(url, path)
