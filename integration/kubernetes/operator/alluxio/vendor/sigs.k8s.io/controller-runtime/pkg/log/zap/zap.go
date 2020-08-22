/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package zap contains helpers for setting up a new logr.Logger instance
// using the Zap logging framework.
package zap

import (
	"io"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New returns a brand new Logger configured with Opts. It
// uses KubeAwareEncoder which adds Type information and
// Namespace/Name to the log.
func New(opts ...Opts) logr.Logger {
	return zapr.NewLogger(NewRaw(opts...))
}

// Logger is a Logger implementation.
// If development is true, a Zap development config will be used
// (stacktraces on warnings, no sampling), otherwise a Zap production
// config will be used (stacktraces on errors, sampling).
//
// Deprecated, use New() and the functional opts pattern instead:
//
// New(func(o *Options){
//    o.Development: development,
// })
func Logger(development bool) logr.Logger {
	return LoggerTo(os.Stderr, development)
}

// LoggerTo returns a new Logger implementation using Zap which logs
// to the given destination, instead of stderr.  It otherwise behaves like
// ZapLogger.
//
// Deprecated, use New() and the functional opts pattern instead:
//
// New(func(o *Options){
//    o.Development: development,
//    o.DestWriter: writer,
// })
func LoggerTo(destWriter io.Writer, development bool) logr.Logger {
	return zapr.NewLogger(RawLoggerTo(destWriter, development))
}

// RawLoggerTo returns a new zap.Logger configured with KubeAwareEncoder
// which logs to a given destination
//
// Deprecated, use NewRaw() and the functional opts pattern instead:
//
// NewRaw(func(o *Options){
//    o.Development: development,
// })
func RawLoggerTo(destWriter io.Writer, development bool, opts ...zap.Option) *zap.Logger {
	o := func(o *Options) {
		o.DestWritter = destWriter
		o.Development = development
		o.ZapOpts = opts
	}
	return NewRaw(o)
}

// Opts allows to manipulate Options
type Opts func(*Options)

// Options contains all possible settings
type Options struct {
	// If Development is true, a Zap development config will be used
	// (stacktraces on warnings, no sampling), otherwise a Zap production
	// config will be used (stacktraces on errors, sampling).
	Development bool
	// The encoder to use, defaults to console when Development is true
	// and JSON otherwise
	Encoder zapcore.Encoder
	// The destination to write to, defaults to os.Stderr
	DestWritter io.Writer
	// The level to use, defaults to Debug when Development is true and
	// Info otherwise
	Level *zap.AtomicLevel
	// StacktraceLevel is the level at and above which stacktraces will
	// be recorded for all messages. Defaults to Warn when Development
	// is true and Error otherwise
	StacktraceLevel *zap.AtomicLevel
	// Raw zap.Options to configure on the underlying zap logger
	ZapOpts []zap.Option
}

// addDefaults adds defaults to the Options
func (o *Options) addDefaults() {
	if o.DestWritter == nil {
		o.DestWritter = os.Stderr
	}

	if o.Development {
		if o.Encoder == nil {
			encCfg := zap.NewDevelopmentEncoderConfig()
			o.Encoder = zapcore.NewConsoleEncoder(encCfg)
		}
		if o.Level == nil {
			lvl := zap.NewAtomicLevelAt(zap.DebugLevel)
			o.Level = &lvl
		}
		if o.StacktraceLevel == nil {
			lvl := zap.NewAtomicLevelAt(zap.WarnLevel)
			o.StacktraceLevel = &lvl
		}
		o.ZapOpts = append(o.ZapOpts, zap.Development())

	} else {
		if o.Encoder == nil {
			encCfg := zap.NewProductionEncoderConfig()
			o.Encoder = zapcore.NewJSONEncoder(encCfg)
		}
		if o.Level == nil {
			lvl := zap.NewAtomicLevelAt(zap.InfoLevel)
			o.Level = &lvl
		}
		if o.StacktraceLevel == nil {
			lvl := zap.NewAtomicLevelAt(zap.ErrorLevel)
			o.StacktraceLevel = &lvl
		}
		o.ZapOpts = append(o.ZapOpts,
			zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewSampler(core, time.Second, 100, 100)
			}))
	}

	o.ZapOpts = append(o.ZapOpts, zap.AddStacktrace(o.StacktraceLevel))
}

// NewRaw returns a new zap.Logger configured with the passed Opts
// or their defaults. It uses KubeAwareEncoder which adds Type
// information and Namespace/Name to the log.
func NewRaw(opts ...Opts) *zap.Logger {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}
	o.addDefaults()

	// this basically mimics New<type>Config, but with a custom sink
	sink := zapcore.AddSync(o.DestWritter)

	o.ZapOpts = append(o.ZapOpts, zap.AddCallerSkip(1), zap.ErrorOutput(sink))
	log := zap.New(zapcore.NewCore(&KubeAwareEncoder{Encoder: o.Encoder, Verbose: o.Development}, sink, *o.Level))
	log = log.WithOptions(o.ZapOpts...)
	return log
}
