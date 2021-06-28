package huawei

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestObsStorage_DeleteObject(t *testing.T) {
	logger := log.NewJSONLogger(os.Stdout)
	tests := []struct {
		name  string
		exist bool
		err   error
	}{
		{
			name:  "Object not Exist",
			exist: false,
			err: obs.ObsError{
				BaseModel: obs.BaseModel{
					StatusCode: http.StatusNotFound,
				},
			},
		},
		{
			name:  "Object Exist",
			exist: true,
			err:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.exist {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}

			}))
			defer srv.Close()
			hwConfig := &ObsStorageConfig{
				Endpoint:  "xxx",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			}
			hwConfig.Endpoint = srv.Listener.Addr().String()
			b, err := NewObsStorage(hwConfig)
			if err != nil {
				level.Debug(logger).Log("new obs storage error")
			}
			err = b.DeleteObject(context.TODO(), "test")
			if !tt.exist {
				level.Debug(logger).Log("name", tt.name, "msg", "not exist")
				obsErr, ok := err.(obs.ObsError)
				actObsErr, _ := tt.err.(obs.ObsError)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, actObsErr.BaseModel.StatusCode, obsErr.BaseModel.StatusCode)
				return
			}
			level.Debug(logger).Log("name", tt.name, "msg", "exist")
			testutil.Ok(t, err)
		})
	}
}

func TestObsStorage_PutObject(t *testing.T) {
	logger := log.NewJSONLogger(os.Stdout)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	hwConfig := &ObsStorageConfig{
		Endpoint:  "xxx",
		Bucket:    "thanos-obs-test",
		AccessKey: "accesskey",
		SecretKey: "secretkey",
	}
	hwConfig.Endpoint = srv.Listener.Addr().String()
	b, err := NewObsStorage(hwConfig)
	if err != nil {
		level.Debug(logger).Log("new obs storage error")
	}
	err = b.PutObject(context.TODO(), "thanos", bytes.NewReader([]byte("thanos")))
	testutil.Ok(t, err)
}

func TestObsStorage_GetObject(t *testing.T) {
	logger := log.NewJSONLogger(os.Stdout)
	tests := []struct {
		name   string
		exist  bool
		err    error
		object string
		length int
	}{
		{
			name:  "Object not Exist",
			exist: false,
			err: obs.ObsError{
				BaseModel: obs.BaseModel{
					StatusCode: http.StatusNotFound,
				},
			},
			object: "",
		},
		{
			name:   "Object Exist",
			exist:  true,
			err:    nil,
			object: "getObject",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.exist {
					_, err := w.Write([]byte("getObject"))
					testutil.Ok(t, err)
					w.WriteHeader(200)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}

			}))
			defer srv.Close()
			hwConfig := &ObsStorageConfig{
				Endpoint:  "xxx",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			}
			hwConfig.Endpoint = srv.Listener.Addr().String()
			b, err := NewObsStorage(hwConfig)
			if err != nil {
				logger.Log("new Bucket error")
			}
			reader, err := b.GetObject(context.TODO(), "test")
			if !tt.exist {
				level.Debug(logger).Log("name", tt.name, "msg", "not exist")
				obsErr, ok := err.(obs.ObsError)
				actObsErr, _ := tt.err.(obs.ObsError)
				testutil.Equals(t, true, ok)
				testutil.Equals(t, actObsErr.BaseModel.StatusCode, obsErr.BaseModel.StatusCode)
				return
			}
			level.Debug(logger).Log("name", tt.name, "msg", "exist")
			testutil.Ok(t, err)
			var data []byte
			// We expect an error when reading back.
			data, _ = ioutil.ReadAll(reader)
			testutil.Equals(t, string(data[:]), tt.object)
		})
	}
}

func TestObsStorage_List(t *testing.T) {
	isNextMarker := true
	logger := log.NewJSONLogger(os.Stdout)
	tests := []struct {
		name             string
		isErr            bool
		isNextMarker     bool
		output           *obs.ListObjectsOutput
		outputNext       *obs.ListObjectsOutput
		outputContentNum int
		outputCommonNum  int
	}{
		{
			name:         "no more data",
			isErr:        false,
			isNextMarker: false,
			output: &obs.ListObjectsOutput{
				Name:   "thanos",
				Prefix: "thanos/",
				Contents: []obs.Content{
					{
						Key: "thanos/file1",
					},
					{
						Key: "thanos/file2",
					},
					{
						Key: "thanos/",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder1",
					"thanos/folder2",
					"thanos/folder3",
				},
			},
			outputNext:       nil,
			outputContentNum: 3,
			outputCommonNum:  3,
		},
		{
			name:         "more data",
			isErr:        false,
			isNextMarker: true,
			output: &obs.ListObjectsOutput{
				Name:        "thanos",
				Prefix:      "thanos/",
				IsTruncated: true,
				NextMarker:  "nextMarker",
				Contents: []obs.Content{
					{
						Key: "thanos/file1",
					},
					{
						Key: "thanos/file2",
					},
					{
						Key: "thanos/",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder1",
					"thanos/folder2",
					"thanos/folder3",
				},
			},
			outputNext: &obs.ListObjectsOutput{
				Name:   "thanos",
				Prefix: "thanos/",
				Contents: []obs.Content{
					{
						Key: "thanos/file3",
					},
					{
						Key: "thanos/file4",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder4",
					"thanos/folder5",
					"thanos/folder6",
				},
			},
			outputContentNum: 5,
			outputCommonNum:  6,
		},
		{
			name:         "err happen",
			isErr:        true,
			isNextMarker: false,
			output: &obs.ListObjectsOutput{
				Name:   "thanos",
				Prefix: "thanos/",
				Contents: []obs.Content{
					{
						Key: "thanos/file1",
					},
					{
						Key: "thanos/file2",
					},
					{
						Key: "thanos/",
					},
				},
				CommonPrefixes: []string{
					"thanos/folder1",
					"thanos/folder2",
					"thanos/folder3",
				},
			},
			outputNext:       nil,
			outputContentNum: 3,
			outputCommonNum:  3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				values := r.URL.Query()
				marker := values.Get("marker")
				var xmlBytes []byte
				var err error
				if marker == "nextMarker" {
					isNextMarker = true
					xmlBytes, err = xml.Marshal(tt.outputNext)
				} else {
					isNextMarker = false
					xmlBytes, err = xml.Marshal(tt.output)
				}
				if err != nil {
					fmt.Fprint(w, err)
				}
				if tt.isErr {
					w.WriteHeader(http.StatusInternalServerError)
				} else {
					w.WriteHeader(200)
					w.Write([]byte(xmlBytes))
				}
			}))
			defer srv.Close()
			hwConfig := &ObsStorageConfig{
				Endpoint:  "xxx",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			}
			hwConfig.Endpoint = srv.Listener.Addr().String()
			b, err := NewObsStorage(hwConfig)
			if err != nil {
				logger.Log("new Bucket error")
			}
			objects, commonPrefix, err := b.List(context.TODO(), "thanos", "/")
			if tt.isErr {
				level.Debug(logger).Log("isErr", "yes")
				if (err != nil) != tt.isErr {
					t.Errorf("Iter() error = %v, isErr %v", err, tt.isErr)
				}
				return
			}
			level.Debug(logger).Log("isErr", "no", "isNextMarker", isNextMarker)
			testutil.Equals(t, tt.isNextMarker, isNextMarker)
			testutil.Equals(t, tt.outputContentNum, len(objects))
			testutil.Equals(t, tt.outputCommonNum, len(commonPrefix))
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	type hwConfig struct {
		Endpoint  string
		Bucket    string
		AccessKey string
		SecretKey string
	}
	tests := []struct {
		name     string
		isErr    bool
		hwConfig hwConfig
	}{
		{
			name:  "normal configuration",
			isErr: false,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing endpoint",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing bucket",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "",
				AccessKey: "accesskey",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing access key",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "thanos-obs-test",
				AccessKey: "",
				SecretKey: "secretkey",
			},
		},
		{
			name:  "missing secret key",
			isErr: true,
			hwConfig: hwConfig{
				Endpoint:  "obs.ap-southeast-2.myhuaweicloud.com",
				Bucket:    "thanos-obs-test",
				AccessKey: "accesskey",
				SecretKey: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &ObsStorageConfig{
				Endpoint:  tt.hwConfig.Endpoint,
				Bucket:    tt.hwConfig.Bucket,
				AccessKey: tt.hwConfig.AccessKey,
				SecretKey: tt.hwConfig.SecretKey,
			}
			err := conf.Validate()
			if (err != nil) != tt.isErr {
				t.Errorf("Config.validate() error = %v, isErr %v", err, tt.isErr)
			}
		})
	}
}
