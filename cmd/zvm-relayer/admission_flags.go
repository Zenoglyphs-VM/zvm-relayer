package main

import (
	"errors"
	"flag"
	"github.com/Zenoglyphs-VM/zvm-relayer/relayer"
)

func (o admissionStoreOptions) Checked() (relayer.AdmissionStoreLimits, error) {
	if o.Limits.MaxRecords <= 0 || o.Limits.MaxRawBytes <= 0 {
		return o.Limits, errors.New("admission retention limits must be positive")
	}
	return o.Limits.Checked()
}

type admissionStoreOptions struct {
	Directory string
	Limits    relayer.AdmissionStoreLimits
}

func admissionStoreFlags(fs *flag.FlagSet) *admissionStoreOptions {
	defaults, _ := (relayer.AdmissionStoreLimits{}).Checked()
	options := &admissionStoreOptions{Limits: defaults}
	fs.StringVar(&options.Directory, "admission-store", "", "private local admission store directory; empty keeps volatile admission")
	fs.IntVar(&options.Limits.MaxRecords, "admission-store-max-records", defaults.MaxRecords, "retained active and retired admission record limit")
	fs.Int64Var(&options.Limits.MaxRawBytes, "admission-store-max-raw-bytes", defaults.MaxRawBytes, "logical retained signed-byte limit; not a physical disk quota")
	return options
}
