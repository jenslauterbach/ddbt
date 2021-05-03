package main

import "testing"

func Test_disableColor(t *testing.T) {
	type args struct {
		disableColor bool
		environment  []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// everything off:
		{"option-false-env-nil", args{disableColor: false, environment: nil}, false},
		{"option-false-env-empty", args{disableColor: false, environment: []string{}}, false},
		{"option-true-env-nil", args{disableColor: true, environment: nil}, true},
		{"option-true-env-empty", args{disableColor: true, environment: []string{}}, true},

		// --no-input=false && individual environment variables "happy path":
		{"option-false-NO_COLOR", args{disableColor: false, environment: []string{"NO_COLOR=true"}}, true},
		{"option-false-DDBT_NO_COLOR", args{disableColor: false, environment: []string{"DDBT_NO_COLOR=true"}}, true},
		{"option-false-TERM=dump", args{disableColor: false, environment: []string{"TERM=dump"}}, true},

		// --no-input=true && individual environment variables "happy path":
		{"option-true-NO_COLOR", args{disableColor: true, environment: []string{"NO_COLOR=true"}}, true},
		{"option-true-DDBT_NO_COLOR", args{disableColor: true, environment: []string{"DDBT_NO_COLOR=true"}}, true},
		{"option-true-TERM=dump", args{disableColor: true, environment: []string{"TERM=dump"}}, true},

		// --no-input=false && multiple environment variables "happy path":
		{"option-false-NO_COLOR_DDBT_NO_COLOR", args{disableColor: false, environment: []string{"NO_COLOR=true", "DDBT_NO_COLOR=true"}}, true},
		{"option-false-NO_COLOR_TERM=dump", args{disableColor: false, environment: []string{"NO_COLOR=true", "TERM=dump"}}, true},
		{"option-false-NO_COLOR_DDBT_NO_COLOR_TERM=dump", args{disableColor: false, environment: []string{"NO_COLOR=true", "DDBT_NO_COLOR=true", "TERM=dump"}}, true},
		{"option-false-DDBT_NO_COLOR_TERM=dump", args{disableColor: false, environment: []string{"DDBT_NO_COLOR=true", "TERM=dump"}}, true},

		// --no-input=true && multiple environment variables "happy path":
		{"option-true-NO_COLOR_DDBT_NO_COLOR", args{disableColor: true, environment: []string{"NO_COLOR=true", "DDBT_NO_COLOR=true"}}, true},
		{"option-true-NO_COLOR_TERM=dump", args{disableColor: true, environment: []string{"NO_COLOR=true", "TERM=dump"}}, true},
		{"option-true-NO_COLOR_DDBT_NO_COLOR_TERM=dump", args{disableColor: true, environment: []string{"NO_COLOR=true", "DDBT_NO_COLOR=true", "TERM=dump"}}, true},
		{"option-true-DDBT_NO_COLOR_TERM=dump", args{disableColor: true, environment: []string{"DDBT_NO_COLOR=true", "TERM=dump"}}, true},

		// TERM!=dump
		{"option-true-TERM=test", args{disableColor: true, environment: []string{"TERM=test"}}, true},
		{"option-false-TERM=test", args{disableColor: false, environment: []string{"TERM=test"}}, false},
		{"option-true-TERM=test_NO_COLOR", args{disableColor: true, environment: []string{"NO_COLOR=true", "TERM=test"}}, true},
		{"option-false-TERM=test_NO_COLOR", args{disableColor: false, environment: []string{"NO_COLOR=true", "TERM=test"}}, true},
		{"option-true-TERM=test_DDBT_NO_COLOR", args{disableColor: true, environment: []string{"DDBT_NO_COLOR=true", "TERM=test"}}, true},
		{"option-false-TERM=test_DDBT_NO_COLOR", args{disableColor: false, environment: []string{"DDBT_NO_COLOR=true", "TERM=test"}}, true},
		{"option-true-TERM=test_DDBT_NO_COLOR_NO_COLOR", args{disableColor: true, environment: []string{"NO_COLOR=true", "DDBT_NO_COLOR=true", "TERM=test"}}, true},
		{"option-false-TERM=test_DDBT_NO_COLOR_NO_COLOR", args{disableColor: false, environment: []string{"NO_COLOR=true", "DDBT_NO_COLOR=true", "TERM=test"}}, true},

		// environment variables with empty and no value
		{"TERM=''", args{disableColor: false, environment: []string{"TERM="}}, false},
		{"TERM", args{disableColor: false, environment: []string{"TERM"}}, false},
		{"NO_COLOR=''", args{disableColor: false, environment: []string{"NO_COLOR="}}, true},
		{"NO_COLOR", args{disableColor: false, environment: []string{"NO_COLOR"}}, true},
		{"DDBT_NO_COLOR=''", args{disableColor: false, environment: []string{"DDBT_NO_COLOR="}}, true},
		{"DDBT_NO_COLOR", args{disableColor: false, environment: []string{"DDBT_NO_COLOR"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := disableColor(tt.args.disableColor, tt.args.environment); got != tt.want {
				t.Errorf("disableColor() = %v, want %v", got, tt.want)
			}
		})
	}
}
