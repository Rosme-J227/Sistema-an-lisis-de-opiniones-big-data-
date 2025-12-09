param(
    [string[]]$Paths = @("src","tools","sql"),
    [string[]]$Exts = @('*.cs','*.sql','*.ps1','*.js','*.cshtml'),
    [switch]$Run
)

function Remove-CommentsFromText {
    param(
        [string]$Text,
        [string]$Ext
    )

    switch ($Ext) {
        '.cs' {

            $Text = [regex]::Replace($Text,'/\*.*?\*/','', [System.Text.RegularExpressions.RegexOptions]::Singleline)

            $Text = [regex]::Replace($Text,'^\s*///.*?$','', [System.Text.RegularExpressions.RegexOptions]::Multiline)

            $Text = [regex]::Replace($Text,'(?m)//.*$','')
        }
        '.sql' {
            $Text = [regex]::Replace($Text,'/\*.*?\*/','', [System.Text.RegularExpressions.RegexOptions]::Singleline)
            $Text = [regex]::Replace($Text,'(?m)--.*$','')
        }
        '.ps1' {
            # Remove block comments: <# ... #>
            $Text = [regex]::Replace($Text,'<#(?s).*?#>','', [System.Text.RegularExpressions.RegexOptions]::Singleline)

            # Remove single-line comments that start with '#' (allow leading whitespace)
            # Preserve shebang (#!) if present
            $Text = [regex]::Replace($Text,'(?m)^(?!\s*#!)\s*#.*$','')
        }
        '.js' {
            $Text = [regex]::Replace($Text,'/\*.*?\*/','', [System.Text.RegularExpressions.RegexOptions]::Singleline)
            $Text = [regex]::Replace($Text,'(?m)//.*$','')
        }
        '.cshtml' {

            $Text = [regex]::Replace($Text,'@\*.*?\*@','', [System.Text.RegularExpressions.RegexOptions]::Singleline)

            $Text = [regex]::Replace($Text,'<!--.*?-->','', [System.Text.RegularExpressions.RegexOptions]::Singleline)

            $Text = [regex]::Replace($Text,'(?m)//.*$','')
        }
        Default {

            $Text = [regex]::Replace($Text,'/\*.*?\*/','', [System.Text.RegularExpressions.RegexOptions]::Singleline)
            $Text = [regex]::Replace($Text,'(?m)//.*$','')
        }
    }


    $Text = [regex]::Replace($Text,'[ \t]+$','', [System.Text.RegularExpressions.RegexOptions]::Multiline)
    return $Text
}

 $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

 $root = Split-Path -Parent $scriptDir
Write-Output "Root: $root"
$modified = @()
$skipped = @()

foreach ($p in $Paths) {
    $abs = Join-Path $root $p
    if (-not (Test-Path $abs)) { continue }
    foreach ($ext in $Exts) {
        Get-ChildItem -Path $abs -Recurse -Include $ext -File -ErrorAction SilentlyContinue | ForEach-Object {
            $file = $_.FullName
            try {
                $orig = Get-Content -Raw -LiteralPath $file -ErrorAction Stop
            } catch {
                $skipped += $file
                continue
            }
            $backup = "$file.bak"
            if (-not (Test-Path $backup)) { Copy-Item -LiteralPath $file -Destination $backup -Force }
            $new = Remove-CommentsFromText -Text $orig -Ext $_.Extension
            if ($new -ne $orig) {
                if ($Run) {
                    Set-Content -LiteralPath $file -Value $new -NoNewline -Encoding UTF8
                }
                $modified += $file
            }
        }
    }
}

Write-Output "Modified files count: $($modified.Count)"
if ($modified.Count -gt 0) { $modified | ForEach-Object { Write-Output " - $_" } }
if ($skipped.Count -gt 0) { Write-Output "Skipped (read errors):"; $skipped | ForEach-Object { Write-Output " - $_" } }

if (-not $Run) {
    Write-Output "Dry run complete. To apply changes rerun with -Run switch." 
}
else {
    Write-Output "Comments removed. Backups created with .bak suffix." 
}
