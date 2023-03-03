package utils

import (
	"io"
	"io/ioutil"
	"os"
	"path"
)

//检查目录或文件是否存在
func DirExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	return false
}

//将目录从 src 复制到 dst
func CopyDir(src string, dst string) error {
	var (
		err     error
		dir     []os.FileInfo
		srcInfo os.FileInfo
	)

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}
	if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}
	if dir, err = ioutil.ReadDir(src); err != nil {
		return err
	}

	for _, fd := range dir {
		srcPath := path.Join(src, fd.Name())
		dstPath := path.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = CopyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err = CopyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

//复制单个文件
func CopyFile(src, dst string) error {
	var (
		err     error
		srcFile *os.File
		dstFie  *os.File
		srcInfo os.FileInfo
	)

	if srcFile, err = os.Open(src); err != nil {
		return err
	}

	defer srcFile.Close()

	if dstFie, err = os.Create(dst); err != nil {
		return err
	}

	defer dstFie.Close()

	if _, err = io.Copy(dstFie, srcFile); err != nil {
		return err
	}

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}

	return os.Chmod(dst, srcInfo.Mode())
}
