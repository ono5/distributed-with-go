package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// レコードサイズとインデックスエントリを永続化するためのエンコーディングを定義
	enc = binary.BigEndian
)

const (
	// レコードの長さを格納するために使うバイト数
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64 // ファイルサイズ
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// ファイルサイズを取得
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// 与えられたbyteをストアに永続化する
// 書き込まれたバイト数、書き込み前のファイルサイズ、エラーを返す
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// レコードの長さを取得
	// レコードを読み出す時何バイト読み込めば良いかわかるようにする
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// ファイルに直接書き込まず、バッファ付きライターに書き込むと直接ファイルに書き込むよりパフォーマンスが良くなる
	// ただし、小さいデータの方が良い
	// 戻り値は書き込まれたバイト数
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	// 書き込んだバイト数を追加する
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// 指定された位置に格納されているレコードを返す
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// レコードをライターバッファへフラッシュ(書き込む)
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	// レコード全体を読み取るために何バイト読まなければならないか調べる
	// posは書き込み前nのファイルサイズ
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	// レコードを読み出して返す
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
