module mosaic-terminal

go 1.22

require (
	github.com/charmbracelet/ssh v0.0.0
	github.com/charmbracelet/wish v0.0.0
)

replace github.com/charmbracelet/ssh => ./third_party/charmbracelet/ssh
replace github.com/charmbracelet/wish => ./third_party/charmbracelet/wish
