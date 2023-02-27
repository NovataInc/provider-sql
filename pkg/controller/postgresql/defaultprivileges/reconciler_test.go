/*
Copyright 2020 The Crossplane Authors.

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

package defaultprivileges

import (
	"context"
	"database/sql"

	"testing"

	"github.com/crossplane-contrib/provider-sql/apis/postgresql/v1alpha1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/crossplane-runtime/pkg/test"

	"github.com/crossplane-contrib/provider-sql/pkg/clients/xsql"
)

type mockDB struct {
	MockExec                 func(ctx context.Context, q xsql.Query) error
	MockExecTx               func(ctx context.Context, ql []xsql.Query) error
	MockScan                 func(ctx context.Context, q xsql.Query, dest ...interface{}) error
	MockScanInt              func(ctx context.Context, q xsql.Query, roleOiD int) error
	MockQuery                func(ctx context.Context, q xsql.Query) (*sql.Rows, error)
	MockGetConnectionDetails func(username, password string) managed.ConnectionDetails
}

func (m mockDB) Exec(ctx context.Context, q xsql.Query) error {
	return m.MockExec(ctx, q)
}
func (m mockDB) ExecTx(ctx context.Context, ql []xsql.Query) error {
	return m.MockExecTx(ctx, ql)
}
func (m mockDB) Scan(ctx context.Context, q xsql.Query, dest ...interface{}) error {
	//test if dest is an int
	if len(dest) == 1 {
		if v, ok := dest[0].(*int); ok {
			return m.MockScanInt(ctx, q, *v)
		}
	}
	return m.MockScan(ctx, q, dest...)
}
func (m mockDB) Query(ctx context.Context, q xsql.Query) (*sql.Rows, error) {
	return m.MockQuery(ctx, q)
}

func (m mockDB) GetConnectionDetails(username, password string) managed.ConnectionDetails {
	return m.MockGetConnectionDetails(username, password)
}

func TestConnect(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		kube  client.Client
		usage resource.Tracker
		newDB func(creds map[string][]byte, database string, sslmode string) xsql.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   error
	}{
		"ErrNotDefaultPrivilege": {
			reason: "An error should be returned if the managed resource is not a *DefaultPrivilege",
			args: args{
				mg: nil,
			},
			want: errors.New(errNot),
		},
		"ErrTrackProviderConfigUsage": {
			reason: "An error should be returned if we can't track our ProviderConfig usage",
			fields: fields{
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return errBoom }),
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{},
			},
			want: errors.Wrap(errBoom, errTrackPCUsage),
		},
		"ErrGetProviderConfig": {
			reason: "An error should be returned if we can't get our ProviderConfig",
			fields: fields{
				kube: &test.MockClient{
					MockGet: test.NewMockGetFn(errBoom),
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ResourceSpec: xpv1.ResourceSpec{
							ProviderConfigReference: &xpv1.Reference{},
						},
					},
				},
			},
			want: errors.Wrap(errBoom, errGetPC),
		},
		"ErrMissingConnectionSecret": {
			reason: "An error should be returned if our ProviderConfig doesn't specify a connection secret",
			fields: fields{
				kube: &test.MockClient{
					// We call get to populate the DefaultPrivilege struct, then again
					// to populate the (empty) ProviderConfig struct, resulting
					// in a ProviderConfig with a nil connection secret.
					MockGet: test.NewMockGetFn(nil),
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ResourceSpec: xpv1.ResourceSpec{
							ProviderConfigReference: &xpv1.Reference{},
						},
					},
				},
			},
			want: errors.New(errNoSecretRef),
		},
		"ErrGetConnectionSecret": {
			reason: "An error should be returned if we can't get our ProviderConfig's connection secret",
			fields: fields{
				kube: &test.MockClient{
					MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
						switch o := obj.(type) {
						case *v1alpha1.ProviderConfig:
							o.Spec.Credentials.ConnectionSecretRef = &xpv1.SecretReference{}
						case *corev1.Secret:
							return errBoom
						}
						return nil
					}),
				},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ResourceSpec: xpv1.ResourceSpec{
							ProviderConfigReference: &xpv1.Reference{},
						},
					},
				},
			},
			want: errors.Wrap(errBoom, errGetSecret),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := &connector{kube: tc.fields.kube, usage: tc.fields.usage, newDB: tc.fields.newDB}
			_, err := e.Connect(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\ne.Connect(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestObserve(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db         xsql.DB
		dbDatabase xsql.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		o   managed.ExternalObservation
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotDefaultPrivilege": {
			reason: "An error should be returned if the managed resource is not a *DefaultPrivilege",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNot),
			},
		},
		"SuccessNoDefaultPrivilege": {
			reason: "We should return ResourceExists: false when no DefaultPrivilege is found",
			fields: fields{
				db: mockDB{
					MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error {
						// Default value is false, so just return
						bv := dest[0].(*bool)
						*bv = false
						return nil
					},
					MockScanInt: func(ctx context.Context, q xsql.Query, roleOiD int) error {

						roleOiD = 0
						return nil
					},
				},
				dbDatabase: mockDB{
					MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error {
						// Default value is false, so just return
						bv := dest[0].(*bool)
						*bv = false
						return nil
					},
					MockScanInt: func(ctx context.Context, q xsql.Query, roleOiD int) error {

						roleOiD = 0
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Database:   pointer.StringPtr("test-example"),
							Role:       pointer.StringPtr("test-example"),
							Owner:      pointer.StringPtr("test-example"),
							Privileges: v1alpha1.DefaultPrivilegePrivileges{"ALL"},
						},
					},
				},
			},
			want: want{
				o: managed.ExternalObservation{ResourceExists: false},
			},
		},
		"ErrSelectDefaultPrivilege": {
			reason: "We should return any errors encountered while trying to show the DefaultPrivilege",
			fields: fields{
				db: mockDB{
					MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error {
						return errBoom
					},
					MockScanInt: func(ctx context.Context, q xsql.Query, roleOiD int) error {

						roleOiD = 0
						return nil
					},
				},
				dbDatabase: mockDB{
					MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error {
						// Default value is false, so just return
						return errBoom
					},
					MockScanInt: func(ctx context.Context, q xsql.Query, roleOiD int) error {

						roleOiD = 0
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Database:   pointer.StringPtr("test-example"),
							Role:       pointer.StringPtr("test-example"),
							Owner:      pointer.StringPtr("test-example"),
							Privileges: v1alpha1.DefaultPrivilegePrivileges{"CONNECT", "TEMPORARY"},
						},
					},
				},
			},
			want: want{
				err: errors.Wrap(errBoom, errSelectDefaultPerms),
			},
		},
		"SuccessDefaultPrivilege": {
			reason: "We should return no error if we can find our role schema DefaultPrivilege",
			fields: fields{
				db: mockDB{
					MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error {
						bv := dest[0].(*bool)
						*bv = true
						return nil
					},
					MockScanInt: func(ctx context.Context, q xsql.Query, roleOiD int) error {

						roleOiD = 0
						return nil
					},
				},
				dbDatabase: mockDB{
					MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error {
						// Default value is false, so just return
						bv := dest[0].(*bool)
						*bv = true
						return nil
					},
					MockScanInt: func(ctx context.Context, q xsql.Query, roleOiD int) error {

						roleOiD = 0
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Role:   pointer.StringPtr("testrole"),
							Schema: pointer.StringPtr("testschema"),
							Owner:  pointer.StringPtr("testowner"),
						},
					},
				},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists:   true,
					ResourceUpToDate: true,
				},
				err: nil,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db, dbDatabase: tc.fields.dbDatabase}
			got, err := e.Observe(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\ne.Observe(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.o, got); diff != "" {
				t.Errorf("\n%s\ne.Observe(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestCreate(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db         xsql.DB
		dbDatabase xsql.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		c   managed.ExternalCreation
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotDefaultPrivilege": {
			reason: "An error should be returned if the managed resource is not a *DefaultPrivilege",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNot),
			},
		},
		"ErrExec": {
			reason: "Any errors encountered while creating the DefaultPrivilege should be returned",
			fields: fields{
				db: &mockDB{
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return errBoom },
				},
				dbDatabase: &mockDB{
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return errBoom },
				},
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Database:   pointer.StringPtr("test-example"),
							Role:       pointer.StringPtr("test-example"),
							Owner:      pointer.StringPtr("test-owner"),
							Schema:     pointer.StringPtr("test-schema"),
							Privileges: v1alpha1.DefaultPrivilegePrivileges{"ALL"},
						},
					},
				},
			},
			want: want{
				err: errors.Wrap(errBoom, errCreateDefaultPerms),
			},
		},
		"Success": {
			reason: "No error should be returned when we successfully create a DefaultPrivilege",
			fields: fields{
				db: &mockDB{
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return nil },
				},
				dbDatabase: &mockDB{
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return nil },
				},
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Database:   pointer.StringPtr("test-example"),
							Role:       pointer.StringPtr("test-example"),
							Owner:      pointer.StringPtr("test-owner"),
							Schema:     pointer.StringPtr("test-schema"),
							Privileges: v1alpha1.DefaultPrivilegePrivileges{"ALL"},
						},
					},
				},
			},
			want: want{
				err: nil,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db, dbDatabase: tc.fields.dbDatabase}
			got, err := e.Create(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\ne.Create(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.c, got); diff != "" {
				t.Errorf("\n%s\ne.Create(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	type fields struct {
		db xsql.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		c   managed.ExternalUpdate
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNoOp": {
			reason: "Update is a no-op, make sure we dont throw an error *DefaultPrivilege",
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Database:   pointer.StringPtr("test-example"),
							Role:       pointer.StringPtr("test-example"),
							Privileges: v1alpha1.DefaultPrivilegePrivileges{"ALL"},
						},
					},
				},
			},
			want: want{
				err: nil,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{
				db: tc.fields.db,
			}
			got, err := e.Update(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\ne.Create(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.c, got, cmpopts.IgnoreMapEntries(func(key string, _ []byte) bool { return key == "password" })); diff != "" {
				t.Errorf("\n%s\ne.Create(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db         xsql.DB
		dbDatabase xsql.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   error
	}{
		"ErrNotDefaultPermission": {
			reason: "An error should be returned if the managed resource is not a *DefaultPermission",
			args: args{
				mg: nil,
			},
			want: errors.New(errNot),
		},
		"ErrDropDefaultPrivilege": {
			reason: "Errors dropping a DefaultPrivilege should be returned",
			fields: fields{
				db: &mockDB{
					MockExec: func(ctx context.Context, q xsql.Query) error {
						return errBoom
					},
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return errBoom },
				},
				dbDatabase: &mockDB{
					MockExec: func(ctx context.Context, q xsql.Query) error {
						return errBoom
					},
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return errBoom },
				},
			},
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Database:   pointer.StringPtr("test-example"),
							Role:       pointer.StringPtr("test-example"),
							Schema:     pointer.StringPtr("test-schema"),
							Owner:      pointer.StringPtr("test-owner"),
							Privileges: v1alpha1.DefaultPrivilegePrivileges{"ALL"},
						},
					},
				},
			},
			want: errors.Wrap(errBoom, errRevokeDefaultPerms),
		},
		"Success": {
			reason: "No error should be returned if the DefaultPrivilege was revoked",
			args: args{
				mg: &v1alpha1.DefaultPrivilege{
					Spec: v1alpha1.DefaultPrivilegeSpec{
						ForProvider: v1alpha1.DefaultPrivilegeParameters{
							Database:   pointer.StringPtr("test-example"),
							Role:       pointer.StringPtr("test-example"),
							Schema:     pointer.StringPtr("test-schema"),
							Owner:      pointer.StringPtr("test-owner"),
							Privileges: v1alpha1.DefaultPrivilegePrivileges{"ALL"},
						},
					},
				},
			},
			fields: fields{
				db: &mockDB{
					MockExec:   func(ctx context.Context, q xsql.Query) error { return nil },
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return nil },
				},
				dbDatabase: &mockDB{
					MockExec:   func(ctx context.Context, q xsql.Query) error { return nil },
					MockExecTx: func(ctx context.Context, ql []xsql.Query) error { return nil },
				},
			},
			want: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db, dbDatabase: tc.fields.dbDatabase}
			err := e.Delete(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\ne.Delete(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
		})
	}
}
