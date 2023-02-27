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
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"

	"github.com/crossplane-contrib/provider-sql/apis/postgresql/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/postgresql"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/xsql"
)

const (
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errNoSecretRef  = "ProviderConfig does not reference a credentials Secret"
	errGetSecret    = "cannot get credentials Secret"

	errNot                     = "managed resource is not a  custom resource"
	errSelectDefaultPerms      = "cannot select default permissions "
	errCreateDefaultPermsQuery = "cannot create default permissions query"
	errSelectRoleId            = "cannot select role id "

	errCreateDefaultPerms      = "cannot create default permissions "
	errRevokeDefaultPerms      = "cannot revoke default permissions "
	errRevokeDefaultPermsQuery = "cannot create revoke default permissions query"
	errNoRole                  = "role not passed or could not be resolved"
	errNoOwner                 = "owner not passed or could not be resolved"
	errNoSchema                = "schema not passed or could not be resolved"
	errNoDatabase              = "database not passed or could not be resolved"
	errNoPrivileges            = "privileges not passed"
	errUnknown                 = "cannot identify  type based on passed params"

	errInvalidParams = "invalid parameters for  type %s"

	maxConcurrency = 5
)

// Setup adds a controller that reconciles  managed resources.
func Setup(mgr ctrl.Manager, l logging.Logger) error {
	name := managed.ControllerName(v1alpha1.DefaultPrivilegeGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.DefaultPrivilegeGroupVersionKind),
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newDB: postgresql.New}),
		managed.WithLogger(l.WithValues("controller", name)),
		managed.WithPollInterval(10*time.Minute),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.DefaultPrivilege{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrency,
		}).
		Complete(r)
}

type connector struct {
	kube  client.Client
	usage resource.Tracker
	newDB func(creds map[string][]byte, database string, sslmode string) xsql.DB
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.DefaultPrivilege)
	if !ok {
		return nil, errors.New(errNot)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	// ProviderConfigReference could theoretically be nil, but in practice the
	// DefaultProviderConfig initializer will set it before we get here.
	pc := &v1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	// We don't need to check the credentials source because we currently only
	// support one source (PostgreSQLConnectionSecret), which is required and
	// enforced by the ProviderConfig schema.
	ref := pc.Spec.Credentials.ConnectionSecretRef
	if ref == nil {
		return nil, errors.New(errNoSecretRef)
	}

	s := &corev1.Secret{}
	if err := c.kube.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, s); err != nil {
		return nil, errors.Wrap(err, errGetSecret)
	}

	crateDb := pc.Spec.DefaultDatabase
	if cr.Spec.ForProvider.Database != nil {
		crateDb = *cr.Spec.ForProvider.Database
	}

	return &external{
		db:         c.newDB(s.Data, pc.Spec.DefaultDatabase, clients.ToString(pc.Spec.SSLMode)),
		dbDatabase: c.newDB(s.Data, crateDb, clients.ToString(pc.Spec.SSLMode)),
		kube:       c.kube,
	}, nil
}

type external struct {
	db         xsql.DB
	kube       client.Client
	dbDatabase xsql.DB
}

func (c *external) GetRoleOID(roleName string) (int, error) {

	var q xsql.Query
	q.String = `SELECT oid FROM pg_roles WHERE rolname = $1;
`
	q.Parameters = []interface{}{
		roleName,
	}

	var oid int
	if err := c.db.Scan(context.Background(), q, &oid); err != nil {
		return 0, fmt.Errorf("could not find oid for role %s: %w", roleName, err)
	}
	return oid, nil
}

func (c *external) DataBaseExits(ctx context.Context, dbNAme string, exists *bool) error {

	var q xsql.Query
	q.String = `SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = $1);
`
	q.Parameters = []interface{}{
		dbNAme,
	}

	if err := c.db.Scan(context.Background(), q, exists); err != nil {
		return fmt.Errorf("could not find database %s: %w", dbNAme, err)
	}
	return nil
}

func selectQuery(q *xsql.Query, ownerID int, roleID int) error {

	q.String = `
	SELECT EXISTS (
	SELECT 1 FROM (
		SELECT defaclnamespace, (aclexplode(defaclacl)).* FROM pg_default_acl
 		WHERE defaclobjtype = 'r'
	) AS t (namespace, grantor_oid, grantee_oid, prtype, grantable)
	JOIN pg_namespace ON pg_namespace.oid = namespace
	WHERE grantee_oid = $1 AND nspname = 'public' AND grantor_oid = $2
   	);
	`
	q.Parameters = []interface{}{
		roleID,
		ownerID,
	}
	return nil

}

func createQueries(gp v1alpha1.DefaultPrivilegeParameters, ql *[]xsql.Query) error { // nolint: gocyclo

	if gp.Role == nil {
		return errors.New(errNoRole)
	}
	if gp.Schema == nil {
		return errors.New(errNoSchema)
	}
	if gp.Privileges == nil {
		return errors.New(errNoPrivileges)
	}

	if gp.Owner == nil {
		return errors.New(errNoOwner)
	}

	ro := pq.QuoteIdentifier(*gp.Role)
	schema := pq.QuoteIdentifier(*gp.Schema)

	p := strings.Join(gp.Privileges.ToStringSlice(), ",")
	if len(p) == 0 {
		return errors.New(errNoPrivileges)
	}

	*ql = append(*ql,
		// REVOKE ANY MATCHING EXISTING PERMISSIONS
		xsql.Query{String: fmt.Sprintf("SET ROLE %s ",
			pq.QuoteIdentifier(*gp.Owner),
		)},
		xsql.Query{String: fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s REVOKE ALL ON TABLES FROM %s",
			pq.QuoteIdentifier(*gp.Owner),
			pq.QuoteIdentifier(*gp.Schema),
			ro,
		)},
		xsql.Query{String: fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s  GRANT  %s ON TABLES TO %s",
			pq.QuoteIdentifier(*gp.Owner),
			schema,
			p,
			ro,
		)},
	)

	return nil

}

func deleteQuery(gp v1alpha1.DefaultPrivilegeParameters, ql *[]xsql.Query) error {
	if gp.Role == nil {
		return errors.New(errNoRole)
	}
	if gp.Schema == nil {
		return errors.New(errNoSchema)
	}

	ro := pq.QuoteIdentifier(*gp.Role)
	*ql = append(*ql,
		xsql.Query{String: fmt.Sprintf("SET ROLE %s ",
			pq.QuoteIdentifier(*gp.Owner),
		)},
		xsql.Query{String: fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s  IN SCHEMA %s REVOKE ALL ON TABLES FROM %s",
			pq.QuoteIdentifier(*gp.Owner),
			pq.QuoteIdentifier(*gp.Schema),
			ro,
		)})

	return nil

}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.DefaultPrivilege)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNot)
	}

	if cr.Spec.ForProvider.Role == nil {
		return managed.ExternalObservation{}, errors.New(errNoRole)
	}

	gp := cr.Spec.ForProvider

	exists := false

	var query xsql.Query
	roleId, err := c.GetRoleOID(*gp.Role)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectRoleId)
	}

	ownerId, err := c.GetRoleOID(*gp.Owner)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectRoleId)
	}

	if err := selectQuery(&query, ownerId, roleId); err != nil {
		return managed.ExternalObservation{}, err
	}

	if err := c.dbDatabase.Scan(ctx, query, &exists); err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectDefaultPerms)
	}

	if !exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	// s have no way of being 'not up to date' - if they exist, they are up to date
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        true,
		ResourceLateInitialized: false,
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.DefaultPrivilege)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNot)
	}

	cr.SetConditions(xpv1.Creating())

	var queries []xsql.Query
	if err := createQueries(cr.Spec.ForProvider, &queries); err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDefaultPermsQuery)
	}
	err := c.dbDatabase.ExecTx(ctx, queries)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDefaultPerms)
	}

	return managed.ExternalCreation{}, errors.Wrap(err, errUnknown)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	// Update is a no-op, as permissions are fully revoked and then ed in the Create function,
	// inside a transaction.
	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.DefaultPrivilege)
	if !ok {
		return errors.New(errNot)
	}

	cr.SetConditions(xpv1.Deleting())

	//check db still exists
	dbExists := false
	if err := c.DataBaseExits(ctx, *cr.Spec.ForProvider.Database, &dbExists); err != nil {
		return errors.Wrap(err, errRevokeDefaultPerms)
	}

	if !dbExists {
		return nil
	}

	var queries []xsql.Query

	if err := deleteQuery(cr.Spec.ForProvider, &queries); err != nil {
		return errors.Wrap(err, errRevokeDefaultPermsQuery)
	}
	err := c.dbDatabase.ExecTx(ctx, queries)
	if err != nil {
		return errors.Wrap(err, errRevokeDefaultPerms)
	}

	return nil
}
