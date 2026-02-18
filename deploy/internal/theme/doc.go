// Package theme resolves typed, immutable style bundles for runtime UI surfaces.
//
// Integration example:
//
//	term := os.Getenv("TERM")
//	bundle, err := theme.Resolve(theme.VariantWest, term)
//	if err != nil {
//		return err
//	}
//	header.SetStyle(bundle.Header)
//	viewport.SetStyle(bundle.Viewport)
//	prompt.SetStyle(bundle.Prompt)
//	warning.SetStyle(bundle.Warning)
//	dossier.SetStyle(bundle.DossierCard)
package theme
