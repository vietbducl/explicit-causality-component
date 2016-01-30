{application, ecc,
 [{description, "A simple ecc implementation"},
  {vsn, "0.1.0"},
  {modules, [
             ecc_app,
             ecc_sup
            ]},
  {registered, [ecc_sup]},
  {applications, [kernel, stdlib]},
  {mod, {ecc_app, []}},
  {env, [
  		{riakport, 8087},
  		{neighbors, ['n2@macbookpro.home']}
  ]}
 ]}.
