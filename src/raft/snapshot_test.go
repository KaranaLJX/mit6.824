package raft

import (
	"reflect"
	"testing"
)

func TestShrinkSlice(t *testing.T) {
	es := []*Entry{
		{
			Index: 1,
		},
		{
			Index: 2,
		},
		{
			Index: 3,
		},
	}
	type args struct {
		ori []*Entry
	}
	tests := []struct {
		name string
		args args
		want []*Entry
	}{
		// TODO: Add test cases.
		{
			name: "test1",
			args: args{
				ori: es[1:],
			},
			want: []*Entry{

				{
					Index: 2,
				},
				{
					Index: 3,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShrinkSlice(tt.args.ori); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ShrinkSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}
