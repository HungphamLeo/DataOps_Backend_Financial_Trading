package configuration

import (
	"reflect"
	"testing"
)

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "multiple broker addresses",
			input: "kafka1:29092,kafka2:29092,kafka3:29092",
			want:  []string{"kafka1:29092", "kafka2:29092", "kafka3:29092"},
		},
		{
			name:  "trims whitespace and ignores empty entries",
			input: " kafka1:29092, , kafka2:29092,",
			want:  []string{"kafka1:29092", "kafka2:29092"},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := splitCSV(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitCSV(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}
