# LIFF Init (Global)

```js
liff.init({ liffId: "2008442168-QM9nPZDr" }).then(async () => {
  if (!liff.isLoggedIn()) { liff.login(); return; }
  const idToken = liff.getIDToken(); // send to /owner/auth/liff/callback
  // POST id_token and sid (from cookie) to Admin callback
});
```
