# sample-api

## Setup

```bash
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.20.0/protoc-3.20.0-osx-x86_64.zip
sudo unzip protoc-3.20.0-osx-x86_64.zip -d /usr/local/protobuf

# .bashrc
# プロトコルバッファ
export PATH="$PATH:/usr/local/protobuf/bin"

go mod init github.com/ono5/api
go get google.golang.org/protobuf/...@v1.28.0

protoc api/v1/*.proto \
	--go_out=. \
	--go_opt=paths=source_relative \
	--proto_path=.
```
