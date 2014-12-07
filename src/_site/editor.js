// Create the queryTextarea editor
window.onload = function() {
  window.editor = CodeMirror.fromTextArea(document.getElementById('queryTextarea'), {
    mode: 'text/x-mysql',
    indentWithTabs: true,
    smartIndent: true,
    lineNumbers: true,
    matchBrackets : true,
    autofocus: true,
    extraKeys: {
      "Ctrl-Space": "autocomplete",
      "Ctrl-Enter": search
    }
  });
};
