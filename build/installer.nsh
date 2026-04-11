!macro customUnInit
  MessageBox MB_YESNO "Do you also want to remove all application data (database, settings, media files)?$\r$\n$\r$\nThis cannot be undone." IDYES removeData IDNO skipData

  removeData:
    nsExec::ExecToLog 'taskkill /F /IM "Digipal Local Server.exe"'
    nsExec::ExecToLog 'taskkill /F /IM "digipal-local-server.exe"'

    Sleep 1000

    RMDir /r "$APPDATA\Digipal Local Server"
    RMDir /r "$APPDATA\digipal-local-server"
    RMDir /r "$LOCALAPPDATA\digipal-local-server-updater"

    Goto done

  skipData:
    nsExec::ExecToLog 'taskkill /F /IM "Digipal Local Server.exe"'
    nsExec::ExecToLog 'taskkill /F /IM "digipal-local-server.exe"'

    Sleep 1000

  done:
!macroend

!macro customInstall
  nsExec::ExecToLog 'taskkill /F /IM "Digipal Local Server.exe"'
  nsExec::ExecToLog 'taskkill /F /IM "digipal-local-server.exe"'
  Sleep 1000
!macroend
