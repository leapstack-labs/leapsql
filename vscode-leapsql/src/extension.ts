import * as path from 'path';
import { workspace, ExtensionContext, window } from 'vscode';

import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind
} from 'vscode-languageclient/node';

let client: LanguageClient | undefined;

export function activate(context: ExtensionContext) {
  const config = workspace.getConfiguration('leapsql');
  const serverPath = config.get<string>('path', 'leapsql');
  const traceLevel = config.get<string>('trace.server', 'off');

  // Server options - spawn leapsql lsp as child process
  const serverOptions: ServerOptions = {
    run: {
      command: serverPath,
      args: ['lsp'],
      transport: TransportKind.stdio
    },
    debug: {
      command: serverPath,
      args: ['lsp'],
      transport: TransportKind.stdio
    }
  };

  // Client options
  const clientOptions: LanguageClientOptions = {
    // Register for .sql files
    documentSelector: [
      { scheme: 'file', language: 'leapsql' },
      { scheme: 'file', pattern: '**/models/**/*.sql' },
      { scheme: 'file', pattern: '**/seeds/**/*.sql' }
    ],
    synchronize: {
      // Watch for changes to .star macro files
      fileEvents: workspace.createFileSystemWatcher('**/*.star')
    },
    outputChannelName: 'LeapSQL',
    traceOutputChannel: window.createOutputChannel('LeapSQL Trace')
  };

  // Create and start the language client
  client = new LanguageClient(
    'leapsql',
    'LeapSQL Language Server',
    serverOptions,
    clientOptions
  );

  // Start the client (also starts the server)
  client.start().catch(err => {
    window.showErrorMessage(`Failed to start LeapSQL language server: ${err.message}`);
  });

  // Watch for configuration changes
  context.subscriptions.push(
    workspace.onDidChangeConfiguration(e => {
      if (e.affectsConfiguration('leapsql.path')) {
        window.showInformationMessage(
          'LeapSQL path changed. Please reload the window for changes to take effect.'
        );
      }
    })
  );
}

export async function deactivate(): Promise<void> {
  if (client) {
    await client.stop();
    client = undefined;
  }
}
