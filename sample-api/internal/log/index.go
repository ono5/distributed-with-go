package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

/*
オフセット：レコードを追加するときに付けられる一意な番号で、レコードのIDとして機能する。
ログのセグメント化：ログはセグメントに分割される。セグメントは、ストアファイルとインデックスファイルのペアで構成される。
ストアファイル：レコードを実際に保存する場所。
インデックスファイル：各レコードへのインデックスを保存し、ストアファイル内のその位置を示す。これにより、ストアへのアクセスが高速化される。
オフセットによるレコードの読み取り：指定されたオフセットのレコードを読み取るには、2つのステップが必要。
インデックスファイルからレコードのエントリを取得し、ストアファイル内でのレコードの位置を見つる。
ストアファイル内のその位置のレコードを読み取る。
インデックスファイルの構造：インデックスファイルは、レコードのオフセットとその格納位置を保存する2つの小さなフィールドから構成される。
メモリへのマップ可能なインデックスファイル：インデックスファイルは小さいため、メモリにマップでき、ファイル操作をメモリ上のデータに対する操作と同じ速度で実行できる。
*/

/*
昔々、ある大きな図書館があった。その図書館には、数えきれないほどの本が収蔵されていた。
そして、その図書館を守るために、特別な仕組みが用意されていた。
まず、図書館に新しい本が追加されるとき、その本には必ず一意な番号が振られた。
これが、本のIDであり、皆からは「オフセット」と呼ばれていた。
オフセットは、その本の特別な名前のようなものであり、図書館の中でその本を見つけるための手がかりとなった。
図書館の管理者たちは、本を整理する方法を考えた。そこで彼らは、大きな本棚をいくつかのセグメントに分割し、
それぞれのセグメントには「ストアファイル」と「インデックスファイル」という2つの部屋が設けられた。
ストアファイルには実際の本が収められ、インデックスファイルにはその本の場所が書かれていた。
図書館の案内役は、皆が欲しい本を素早く見つけられるように、手助けをしていた。まず、彼らはオフセットを調べ、
その本の場所が書かれたインデックスファイルを見つけた。そして次に、その本の場所が書かれた場所に行き、本を手に取った。
そのようにして、図書館の管理者たちは、本を整理し、皆が簡単に本を見つけられるようにした。
そして、彼らはインデックスファイルを小さくすることで、検索をより速く行えるようにした。
インデックスファイルは、まるで魔法の地図のように、図書館の本の場所を示してくれたのだ。
*/

const (
	// 以下はインデックスエントリを構成するバイト数を定義
	offWidth uint64 = 4                   // オフセット
	posWidth uint64 = 8                   // ストアファイル内の位置
	entWidth        = offWidth + posWidth // ファイル内の位置
)

// インデックスファイル(永続化されたファイルとメモリマップされたファイルで構成)
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // インデックスのサイズ
}

type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}

// 指定したファイルのインデックスを作る
// インデックスを作成し、ファイルの現在のサイズを保存
// これにより、インデックスファイル内のデータ量を管理できるようになる
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	//　ファイルを最大のインデックスファイルまで大きくする
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// メモリにマップされたファイルのデータを永続化されたファイルへ同期
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// 永続化されたファイルの内容を安定したストレージへ同期
	if err := i.file.Sync(); err != nil {
		return err
	}

	// 永続化されたファイルのその中にある実際のデータ量まで切り詰める
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	// ファイルを閉じる
	return i.file.Close()
}

// ストア内に関連したレコードの位置を返す
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// 与えられたオフセットと位置をインデックスに追加する
func (i *index) Write(off uint32, pos uint64) error {
	if i.isMaxed() {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}
