package sst

import "errors"

// SST related errors
var (
	ErrInvalidMetadata  = errors.New("invalid metadata format")
	ErrInvalidChecksum  = errors.New("invalid checksum")
	ErrInvalidSSTFile   = errors.New("invalid SST file format")
	ErrSSTNotFound      = errors.New("SST file not found")
	ErrBlockNotFound    = errors.New("block not found")
	ErrEmptySST         = errors.New("cannot build empty SST")
	ErrInvalidBlockSize = errors.New("invalid block size")
	ErrCorruptedFile    = errors.New("corrupted SST file")
)
