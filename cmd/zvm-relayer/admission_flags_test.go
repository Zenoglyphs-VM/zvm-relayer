package main

import (
	"flag"
	"strings"
	"testing"
)

func TestAdmissionStoreFlagsAndEarlyRefusals(t *testing.T) {
	for _, args := range [][]string{{"-admission-store-max-records=0"}, {"-admission-store-max-records=-1"},
		{"-admission-store-max-records=262145"}, {"-admission-store-max-raw-bytes=0"}, {"-admission-store-max-raw-bytes=-1"},
		{"-admission-store-max-raw-bytes=1073741825"}} {
		err := runRelayer(append([]string{"-config=unused-invalid-configuration"}, args...))
		if err == nil || !strings.Contains(err.Error(), "admission retention") {
			t.Fatal("invalid retention did not precede configuration and network", args, err)
		}
	}
	fs := flag.NewFlagSet("admission-controls", flag.ContinueOnError)
	options := admissionStoreFlags(fs)
	if err := fs.Parse([]string{"-admission-store=private-admissions", "-admission-store-max-records=7", "-admission-store-max-raw-bytes=4096"}); err != nil {
		t.Fatal(err)
	}
	checked, err := options.Checked()
	if err != nil || options.Directory != "private-admissions" || checked.MaxRecords != 7 || checked.MaxRawBytes != 4096 {
		t.Fatal("explicit retention options not preserved")
	}
}
