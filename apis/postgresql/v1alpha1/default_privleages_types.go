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

package v1alpha1

import (
	"context"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/errors"
	"github.com/crossplane/crossplane-runtime/pkg/reference"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A DefaultPrivilegeSpec defines the desired state of a DefaultPrivilege.
type DefaultPrivilegeSpec struct {
	xpv1.ResourceSpec `json:",inline"`
	ForProvider       DefaultPrivilegeParameters `json:"forProvider"`
}

// DefaultPrivilegePrivilege represents a privilege to be DefaultPrivilegeed
// +kubebuilder:validation:Pattern:=^[A-Z]+$
type DefaultPrivilegePrivilege string

// If Privileges are specified, we should have at least one

// DefaultPrivilegePrivileges is a list of the privileges to be DefaultPrivilegeed
// +kubebuilder:validation:MinItems:=1
type DefaultPrivilegePrivileges []DefaultPrivilegePrivilege

// ToStringSlice converts the slice of privileges to strings
func (gp *DefaultPrivilegePrivileges) ToStringSlice() []string {
	if gp == nil {
		return []string{}
	}
	out := make([]string, len(*gp))
	for i, v := range *gp {
		out[i] = string(v)
	}
	return out
}

// DefaultPrivilegeParameters define the desired state of a PostgreSQL DefaultPrivilege instance.
type DefaultPrivilegeParameters struct {
	// Privileges to be DefaultPrivilegeed.
	// See https://www.postgresql.org/docs/current/sql-DefaultPrivilege.html for available privileges.
	// +optional
	Privileges DefaultPrivilegePrivileges `json:"privileges,omitempty"`

	// Role this DefaultPrivilege is for.
	// +optional
	Role *string `json:"role,omitempty"`

	// Role this DefaultPrivilege is for.
	// +optional
	Owner *string `json:"owner,omitempty"`

	// RoleRef references the role object this DefaultPrivilege is for.
	// +immutable
	// +optional
	OwnerRef *xpv1.Reference `json:"ownerRef,omitempty"`

	// RoleSelector selects a reference to a Role this DefaultPrivilege is for.
	// +immutable
	// +optional
	OwnerSelector *xpv1.Selector `json:"ownerSelector,omitempty"`

	// Schema this DefaultPrivilege is for.
	// +required
	Schema *string `json:"schema,omitempty"`

	// RoleRef references the role object this DefaultPrivilege is for.
	// +immutable
	// +optional
	RoleRef *xpv1.Reference `json:"roleRef,omitempty"`

	// RoleSelector selects a reference to a Role this DefaultPrivilege is for.
	// +immutable
	// +optional
	RoleSelector *xpv1.Selector `json:"roleSelector,omitempty"`

	// Database this DefaultPrivilege is for.
	// +optional
	Database *string `json:"database,omitempty"`

	// DatabaseRef references the database object this DefaultPrivilege it for.
	// +immutable
	// +optional
	DatabaseRef *xpv1.Reference `json:"databaseRef,omitempty"`

	// DatabaseSelector selects a reference to a Database this DefaultPrivilege is for.
	// +immutable
	// +optional
	DatabaseSelector *xpv1.Selector `json:"databaseSelector,omitempty"`
}

// A DefaultPrivilegeStatus represents the observed state of a DefaultPrivilege.
type DefaultPrivilegeStatus struct {
	xpv1.ResourceStatus `json:",inline"`
}

// +kubebuilder:object:root=true

// A DefaultPrivilege represents the declarative state of a PostgreSQL DefaultPrivilege.
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="ROLE",type="string",JSONPath=".spec.forProvider.role"
// +kubebuilder:printcolumn:name="DATABASE",type="string",JSONPath=".spec.forProvider.database"
// +kubebuilder:printcolumn:name="PRIVILEGES",type="string",JSONPath=".spec.forProvider.privileges"
// +kubebuilder:printcolumn:name="SCHEMA",type="string",JSONPath=".spec.forProvider.schema"
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,sql}
type DefaultPrivilege struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DefaultPrivilegeSpec   `json:"spec"`
	Status DefaultPrivilegeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DefaultPrivilegeList contains a list of DefaultPrivilege
type DefaultPrivilegeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DefaultPrivilege `json:"items"`
}

//
//// ResolveReferences of this DefaultPrivilege
func (mg *DefaultPrivilege) ResolveReferences(ctx context.Context, c client.Reader) error {
	r := reference.NewAPIResolver(c, mg)

	// Resolve spec.forProvider.database
	rsp, err := r.Resolve(ctx, reference.ResolutionRequest{
		CurrentValue: reference.FromPtrValue(mg.Spec.ForProvider.Database),
		Reference:    mg.Spec.ForProvider.DatabaseRef,
		Selector:     mg.Spec.ForProvider.DatabaseSelector,
		To:           reference.To{Managed: &Database{}, List: &DatabaseList{}},
		Extract:      reference.ExternalName(),
	})
	if err != nil {
		return errors.Wrap(err, "spec.forProvider.database")
	}
	mg.Spec.ForProvider.Database = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.ForProvider.DatabaseRef = rsp.ResolvedReference

	// Resolve spec.forProvider.role
	rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
		CurrentValue: reference.FromPtrValue(mg.Spec.ForProvider.Role),
		Reference:    mg.Spec.ForProvider.RoleRef,
		Selector:     mg.Spec.ForProvider.RoleSelector,
		To:           reference.To{Managed: &Role{}, List: &RoleList{}},
		Extract:      reference.ExternalName(),
	})
	if err != nil {
		return errors.Wrap(err, "spec.forProvider.role")
	}
	mg.Spec.ForProvider.Role = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.ForProvider.RoleRef = rsp.ResolvedReference

	// Resolve spec.forProvider.owner
	rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
		CurrentValue: reference.FromPtrValue(mg.Spec.ForProvider.Owner),
		Reference:    mg.Spec.ForProvider.OwnerRef,
		Selector:     mg.Spec.ForProvider.OwnerSelector,
		To:           reference.To{Managed: &Role{}, List: &RoleList{}},
		Extract:      reference.ExternalName(),
	})
	if err != nil {
		return errors.Wrap(err, "spec.forProvider.owner")
	}
	mg.Spec.ForProvider.Owner = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.ForProvider.OwnerRef = rsp.ResolvedReference

	return nil
}
