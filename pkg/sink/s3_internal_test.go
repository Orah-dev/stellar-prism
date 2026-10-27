package sink

import (
	"testing"
	"time"
)

// TestAWSAuthorizationMatchesAWSExample checks the signer against the PUT
// Object example in the AWS S3 documentation. It includes extra signed headers
// (date, x-amz-storage-class), which is what makes it a real test of
// canonicalisation rather than of a single fixed request shape.
func TestAWSAuthorizationMatchesAWSExample(t *testing.T) {
	t.Parallel()

	got := awsAuthorization(awsSigningInput{
		method:       "PUT",
		canonicalURI: "/test%24file.text",
		headers: map[string]string{
			"date":                 "Fri, 24 May 2013 00:00:00 GMT",
			"host":                 "examplebucket.s3.amazonaws.com",
			"x-amz-storage-class":  "REDUCED_REDUNDANCY",
			"x-amz-content-sha256": "ignored-and-recomputed",
		},
		payload:   []byte("Welcome to Amazon S3."),
		accessKey: "AKIAIOSFODNN7EXAMPLE",
		secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		region:    "us-east-1",
		service:   "s3",
		now:       time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC),
	})

	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request," +
		"SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class," +
		"Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
	if got != want {
		t.Errorf("authorization mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestAWSURIEncodeSlashOption(t *testing.T) {
	t.Parallel()

	if got := awsURIEncode("a b/c$d~", false); got != "a%20b/c%24d~" {
		t.Errorf("awsURIEncode() = %q", got)
	}
	if got := awsURIEncode("a/b", true); got != "a%2Fb" {
		t.Errorf("awsURIEncode(encodeSlash) = %q", got)
	}
}

func TestBackoffDelay(t *testing.T) {
	t.Parallel()

	if got := backoffDelay(100*time.Millisecond, time.Second, 1, 0); got != 100*time.Millisecond {
		t.Errorf("attempt 1 = %s, want 100ms", got)
	}
	if got := backoffDelay(100*time.Millisecond, time.Second, 3, 0); got != 400*time.Millisecond {
		t.Errorf("attempt 3 = %s, want 400ms", got)
	}
	if got := backoffDelay(100*time.Millisecond, time.Second, 10, 0); got != time.Second {
		t.Errorf("attempt 10 = %s, want the cap", got)
	}
	if got := backoffDelay(2*time.Second, time.Second, 1, 0); got != time.Second {
		t.Errorf("initial above the cap = %s, want the cap", got)
	}
	if got := backoffDelay(100*time.Millisecond, 150*time.Millisecond, 2, 0); got != 150*time.Millisecond {
		t.Errorf("attempt 2 with a narrow cap = %s, want the cap", got)
	}

	// Jitter stays within the configured fraction of the base delay.
	for range 100 {
		got := backoffDelay(100*time.Millisecond, time.Second, 1, 0.2)
		if got < 80*time.Millisecond || got > 120*time.Millisecond {
			t.Fatalf("jittered delay = %s, want within +/-20%% of 100ms", got)
		}
	}
}
