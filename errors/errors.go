package errors

import (
	"errors"
	"fmt"
)

// PanicError defines an error when occurred a panic error.
type PanicError struct {
	ErrMsg string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic error cause by: %s", e.ErrMsg)
}

/*************************************************
    Standard Library errors package functions
*************************************************/

// New returns an error that formats as the given text.
// Each call to New returns a distinct error value even if the text is identical.
//
// This function is the errors.New function from the standard library (https://golang.org/pkg/errors/#New).
// It is exported from this package for import convenience.
func New(text string) error { return errors.New(text) }

// Is reports whether any error in err's chain matches target.
//
// This function is the errors.Is function from the standard library (https://golang.org/pkg/errors/#Is).
// It is exported from this package for import convenience.
func Is(err, target error) bool { return errors.Is(err, target) }

// As finds the first error in err's chain that matches target, and if so, sets target to that error value and returns true.
// Otherwise, it returns false.
//
// This function is the errors.As function from the standard library (https://golang.org/pkg/errors/#As).
// It is exported from this package for import convenience.
func As(err error, target interface{}) bool { return errors.As(err, target) }

// Unwrap returns the result of calling the Unwrap method on err, if err's type contains an Unwrap method returning error.
// Otherwise, Unwrap returns nil.
//
// This function is the errors.Unwrap function from the standard library (https://golang.org/pkg/errors/#Unwrap).
// It is exported from this package for import convenience.
func Unwrap(err error) error { return errors.Unwrap(err) }
