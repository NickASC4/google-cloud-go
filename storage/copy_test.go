// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"strings"
	"testing"
)

func TestCopyMissingFields(t *testing.T) {
	// Verify that copying checks for missing fields.
	t.Parallel()
	var tests = []struct {
		desc                                     string
		srcBucket, srcName, destBucket, destName string
		errMsg                                   string
	}{
		{
			"missing source name",
			"mybucket", "", "mybucket", "destname",
			"storage: object name is empty",
		},
		{
			"missing destination name",
			"mybucket", "srcname", "mybucket", "",
			"storage: object name is empty",
		},
		{
			"missing source bucket",
			"", "srcfile", "mybucket", "destname",
			"storage: bucket name is empty",
		},
		{
			"missing destination bucket",
			"mybucket", "srcfile", "", "destname",
			"storage: bucket name is empty",
		},
	}
	ctx := context.Background()
	client := mockClient(t, &mockTransport{})
	for _, test := range tests {
		src := client.Bucket(test.srcBucket).Object(test.srcName)
		dst := client.Bucket(test.destBucket).Object(test.destName)
		_, err := dst.CopierFrom(src).Run(ctx)
		if err == nil {
			t.Errorf("CopyTo %s: got nil, want error", test.desc)
			continue
		}
		if !strings.Contains(err.Error(), test.errMsg) {
			t.Errorf("CopyTo %s:\ngot err  %q\nwant err %q", test.desc, err, test.errMsg)
		}
	}
}

func TestCopyBothEncryptionKeys(t *testing.T) {
	// Test that using both a customer-supplied key and a KMS key is an error.
	ctx := context.Background()
	client := mockClient(t, &mockTransport{})
	dest := client.Bucket("b").Object("d").Key(testEncryptionKey)
	c := dest.CopierFrom(client.Bucket("b").Object("s"))
	c.DestinationKMSKeyName = "key"
	if _, err := c.Run(ctx); err == nil {
		t.Error("got nil, want error")
	} else if !strings.Contains(err.Error(), "KMS") {
		t.Errorf(`got %q, want it to contain "KMS"`, err)
	}
}
