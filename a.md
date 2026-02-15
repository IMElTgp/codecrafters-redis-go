when he said that he loved his life

valder fields

Package filepath implements utility routines for manipulating filename paths in a way compatible with the target operating system-defined file paths.

The filepath package uses either forward slashes or backslashes, depending on the operating system. To process paths such as URLs that always use forward slashes regardless of the operating system, see the path package.

const Separator = os.PathSeparator
var ErrBadPattern = errors.New("syntax error in pattern")
var SkipAll error = fs.SkipAll
type WalkFunc func(path string, info fs.FileInfo, err error) error

The path package should only be used for paths separated by forward slashes, such as the paths in URLs. This package does not deal with Windows paths with drive letters or backslashes; to manipulate operating system paths, use the path/filepath package.

var ErrBadPattern = errors.New("syntax error in pattern")

func Split(path string) (dir, file string)
func Join(elem ...string) string

Package errors implements functions to manipulate errors.

The New function creates errors whose only content is a text message.

An error e wraps another error if e's type has one of the methods

Unwrap() error
Unwrap() []error

If e.Unwrap() returns a non-nil error w or a slice containing w, then we say that e wraps w. A nil error returned from e.Unwrap() indicates that e does not wrap any error. It is invalid for an Unwrap method to return an []error containing a nil error value.

An easy way to create wrapped errors is to call fmt.Errorf and apply the %w verb to the error argument:

wrapsErr := fmt.Errorf("... %w ...", ..., err, ...)

Successive unwrapping of an error creates a tree. The is and As functions inspect an error's tree by examining first the error itself followed by the tree of each of its children in turn (pre-order, depth-first traversal).

See https://go.dev/blog/go1.13-errors for a deeper discussion of the philosophy of wrapping and when to wrap.

Is examines the tree of its first argument looking for an error that matches the second. It reports whether it finds a match. It should be used in preference to simple equality checks:

if errors.Is(err, fs.ErrExist)

if preferable to 

if err == fs.ErrExist

because the former will succeed if err wraps io/fs.ErrExist.

As examines the tree of its first argument looking for an error that can be assigned to its second argument, which must be a pointer. If it succeeds, it performs the assignment and returns true. Otherwise, it returns false. The form 

var perr *fs.PathError
if errors.As(err, &perr) {
    fmt.Println(perr.Path)
}

is preferable to 

if perr, ok := err.(*fs.PathError); ok {
    fmt.Println(perr.Path)
}

because the former will succeed if err wraps an *io/fs.PathError.

yell out loud

https://go.dev/blog/go1.13-errors

Is examines the tree of its first argument looking for an error that matches the second. It reports whether it finds a match. It should be used in preference to simple equality checks:

if errors.Is(err, fs.ErrExist)

is preferable to:

if err == fs.ErrExist

because the former will succeed if err wraps io/fs.ErrExist.

examine

func Join(errs ...error) bool

your_program.sh

your_program.sh

your_program.sh

shanghai

peking

beijing

UCAS

if perr, ok := err.(*fs.PathError); ok {
    fmt.Println(perr.Path)
}

Package log implements a simple logging package. It defines a type, Logger, with methods for formatting output. It also has a predefined 'standard' Logger accessible through helper functions Print[f|ln], Fatal[f|ln], and Panic[f|ln], which are easier to use than creating a Logger manually. That logger writes to standard error and prints the date and time of each logged message. Every log message is output on a separate line: if the message being printed does not end in a newline, the logger will add one. The Fatal functions call os.Exit(1) after writing the log message. The Panic functions call panic after writing the log message.

const Ldate = 1 << iota...
func Fatal(v ...any)
func Fatalf(format string, v ...any)
func Fatalln(v ...any)
func Flags() int
func Output(calldepth int, s string) error
func Panic(v ...any)
func Panicf(format string, v ...any)
func Panicln(v ...any)
func Prefix() string
func Print(v ...any)
func Printf(format string, v ...any)
func Println(v ...any)

Package unsafe contains operations that step around the type safety of Go programs.

Packages that import unsafe may be non-portable and are not protected by the Go 1 compatibility guidelines.

func Alignof(x ArbitraryType) uintptr

func Offsetof(x ArbitraryType) uintptr
func Sizeof(x ArbitraryType) uintptr
func String(ptr *byte, len IntegerType) string

type ArbitraryType

arbitrary

arbitrary

arbitrary

arbitrary

arbitraryType

arbitraryType

ArbitraryType

ArbitraryType

Arbitrary

unsafe.Pointer

unsafe.Slice

unsafe.Slice

unsafe.Slice

unsafe.Pointer
unsafe.Pointer

unsafe.Pointer

pointerpointer

Pointer

Pointer

unsafe.Pointer

unsafe.Pointer

Pointer

uintptr

uintptr

pointer

arbitraryType

arbitrarytype

offsetof

your_program.sh

your_program.sh

README.md

codecrafters.yml

Package reflect implements run-time reflections, allowing a program to manipulate objects with arbitrary types. The typical use is to taks a value with static type interface{} and extract its dynamic type information by calling TypeOf, which returns a Type.

A call to ValueOf returns a Value representing the run-time data. Zero takes a Type and returns a Value representing a zero value for that type.

See "The Laws of Reflection" for an introduction to reflection in Go:
https://golang.org/doc/articles/laws_of_reflection.html

const Ptr = Pointer

func Copy(dst, src Value) int
func DeepEqual(x, y any) bool

func Select(cases []SelectCase) (chosen int, recv Value, recvOK bool)
func Swapper(slice any) func(i, j int)
func TypeAssert[T any](v Value) (T bool)

const RecvDir ChanDir = 1 << iota ...

type Kind uint

const Invalid Kind = iota ...

type MapIter struct{ ... }

BUG: FieldByName and related functions consider struct field names to be equal if the names are equal, even if they are unexported names originating in different packages. The practical effect of this is that the result of t.FieldByName("x") is not well defined if the struct type t contains multiple fields named x (embedded from different packages).

FieldByName may return one of the fields named x or may report that there are none.
See https://golang.org/issue/4876 for more details.

Package poll supports non-blocking I/O on file descriptors with polling. This supports I/O operations that block only a goroutine, not a thread. This is used by the net and os packages. It uses a poller built into the runtime, with support from the runtime scheduler.


func Splice(dst, src *FD, remain int64) (written int64, handled bool, err error)

func DupCloseOnExec(fd int) (int, string, error)

type DeadlineExceededError struct{}

type SysFile struct{ ... }

implementations implementations implementations

primitives primitives primitives

package

interfaces

interfaces

otherwise

otherwise

informed

clients 

should

assume 

various

implementations

implementations

implementations

implementations

implementation

implementation

lower-level

lower-level

SeekStart

implementations

parallel

execution

public

public

public

public

public

primitives

related

job

Package io provides basic interfaces to I/O primitives. Its primary job is to wrap existing