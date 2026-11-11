#region Licence

/* The MIT License (MIT)
Copyright © 2026 gabisonia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System.Text.Json;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

var entries = JsonSerializer.Deserialize<List<Input>>(File.ReadAllText(args[0]))!;
var results = entries.Select(entry =>
{
    var before = Surface(entry.Before);
    var after = Surface(entry.After);
    return new { entry.File, RemovedOrChanged = before.Except(after).ToArray(), AddedOrChanged = after.Except(before).ToArray() };
}).ToArray();
File.WriteAllText(args[1], JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
Console.WriteLine($"Audited {entries.Count} source files; {results.Sum(r => r.RemovedOrChanged.Length)} removed/changed declarations require review; {results.Sum(r => r.AddedOrChanged.Length)} added/changed declarations.");

static HashSet<string> Surface(string text)
{
    var result = new HashSet<string>();
    var root = CSharpSyntaxTree.ParseText(text).GetRoot();
    foreach (var member in root.DescendantNodes().OfType<MemberDeclarationSyntax>())
    {
        var enclosing = member.Ancestors().OfType<BaseTypeDeclarationSyntax>().ToArray();
        if (enclosing.Any(t => !Visible(t.Modifiers))) continue;
        var ns = string.Join(".", member.Ancestors().OfType<BaseNamespaceDeclarationSyntax>().Reverse().Select(n => n.Name.ToString()));
        var owner = ns + "." + string.Join(".", enclosing.Reverse().Select(t => t.Identifier.Text));
        var identity = member switch
        {
            TypeDeclarationSyntax t when Visible(t.Modifiers) => $"type {t.Keyword.Text} {t.Identifier}{Text(t.TypeParameterList)} : {Text(t.BaseList)} {string.Join(" ", t.ConstraintClauses.Select(Text))}",
            EnumDeclarationSyntax e when Visible(e.Modifiers) => $"enum {e.Identifier} : {Text(e.BaseList)}",
            ConstructorDeclarationSyntax c when Visible(c.Modifiers) => $"ctor {c.Identifier}{Text(c.ParameterList)}",
            MethodDeclarationSyntax m when Visible(m.Modifiers) || enclosing.FirstOrDefault() is InterfaceDeclarationSyntax => $"method {Text(m.ReturnType)} {m.Identifier}{Text(m.TypeParameterList)}{Text(m.ParameterList)} {string.Join(" ", m.ConstraintClauses.Select(Text))}",
            PropertyDeclarationSyntax p when Visible(p.Modifiers) || enclosing.FirstOrDefault() is InterfaceDeclarationSyntax => $"property {Text(p.Type)} {p.Identifier} [{string.Join(",", p.AccessorList?.Accessors.Select(a => Tokens(a.Modifiers) + a.Keyword.Text) ?? (p.ExpressionBody != null ? ["get"] : []))}]",
            DelegateDeclarationSyntax d when Visible(d.Modifiers) => $"delegate {Text(d.ReturnType)} {d.Identifier}{Text(d.TypeParameterList)}{Text(d.ParameterList)}",
            FieldDeclarationSyntax f when Visible(f.Modifiers) => $"field {Text(f.Declaration.Type)} {string.Join(",",f.Declaration.Variables.Select(v => v.Identifier.Text + (f.Modifiers.Any(SyntaxKind.ConstKeyword) ? "=" + Text(v.Initializer) : "")))}",
            EventFieldDeclarationSyntax e when Visible(e.Modifiers) => $"event {Text(e.Declaration.Type)} {string.Join(",",e.Declaration.Variables.Select(v => v.Identifier.Text))}",
            IndexerDeclarationSyntax i when Visible(i.Modifiers) => $"indexer {Text(i.Type)} {Text(i.ParameterList)} [{string.Join(",",i.AccessorList?.Accessors.Select(a => Tokens(a.Modifiers) + a.Keyword.Text) ?? [])}]",
            OperatorDeclarationSyntax o when Visible(o.Modifiers) => $"operator {Text(o.ReturnType)} {o.OperatorToken.Text}{Text(o.ParameterList)}",
            ConversionOperatorDeclarationSyntax c when Visible(c.Modifiers) => $"conversion {c.ImplicitOrExplicitKeyword.Text} {Text(c.Type)}{Text(c.ParameterList)}",
            EventDeclarationSyntax e when Visible(e.Modifiers) => $"event {Text(e.Type)} {e.Identifier.Text}",
            _ => null
        };
        var modifiers = member switch
        {
            BaseTypeDeclarationSyntax t => t.Modifiers,
            BaseMethodDeclarationSyntax m => m.Modifiers,
            BasePropertyDeclarationSyntax p => p.Modifiers,
            BaseFieldDeclarationSyntax f => f.Modifiers,
            DelegateDeclarationSyntax d => d.Modifiers,
            _ => default
        };
        if (identity != null) result.Add(owner + "::" + string.Join(" ", modifiers.Where(t => !t.IsKind(SyntaxKind.AsyncKeyword) && !t.IsKind(SyntaxKind.PartialKeyword)).Select(t => t.Text)) + " " + identity);
    }
    return result;
}
static bool Visible(SyntaxTokenList modifiers) => modifiers.Any(SyntaxKind.PublicKeyword) || modifiers.Any(SyntaxKind.ProtectedKeyword);
static string Text(SyntaxNode? node) => node?.NormalizeWhitespace().ToFullString() ?? "";
static string Tokens(SyntaxTokenList tokens) => string.Join(" ", tokens.Select(t => t.Text));
record Input(string File, string Before, string After);
