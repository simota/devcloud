import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'

type DynamoDBOperationFormsProps = {
  activeTableName?: string
  busyOperation?: string
  createTableJSON: string
  putItemJSON: string
  updateItemJSON: string
  deleteItemJSON: string
  ttlJSON: string
  deleteTableJSON: string
  deleteItemConfirmation: string
  deleteTableConfirmation: string
  deleteItemAcknowledged: boolean
  deleteTableAcknowledged: boolean
  onCreateTable: (event: FormEvent<HTMLFormElement>) => void
  onPutItem: (event: FormEvent<HTMLFormElement>) => void
  onUpdateItem: (event: FormEvent<HTMLFormElement>) => void
  onDeleteItem: (event: FormEvent<HTMLFormElement>) => void
  onUpdateTTL: (event: FormEvent<HTMLFormElement>) => void
  onDeleteTable: (event: FormEvent<HTMLFormElement>) => void
  setCreateTableJSON: (value: string) => void
  setPutItemJSON: (value: string) => void
  setUpdateItemJSON: (value: string) => void
  setDeleteItemJSON: (value: string) => void
  setTTLJSON: (value: string) => void
  setDeleteTableJSON: (value: string) => void
  setDeleteItemConfirmation: (value: string) => void
  setDeleteTableConfirmation: (value: string) => void
  setDeleteItemAcknowledged: (value: boolean) => void
  setDeleteTableAcknowledged: (value: boolean) => void
}

export function DynamoDBOperationForms({
  activeTableName,
  busyOperation,
  createTableJSON,
  putItemJSON,
  updateItemJSON,
  deleteItemJSON,
  ttlJSON,
  deleteTableJSON,
  deleteItemConfirmation,
  deleteTableConfirmation,
  deleteItemAcknowledged,
  deleteTableAcknowledged,
  onCreateTable,
  onPutItem,
  onUpdateItem,
  onDeleteItem,
  onUpdateTTL,
  onDeleteTable,
  setCreateTableJSON,
  setPutItemJSON,
  setUpdateItemJSON,
  setDeleteItemJSON,
  setTTLJSON,
  setDeleteTableJSON,
  setDeleteItemConfirmation,
  setDeleteTableConfirmation,
  setDeleteItemAcknowledged,
  setDeleteTableAcknowledged,
}: DynamoDBOperationFormsProps): JSX.Element {
  const tableActionDisabled = !activeTableName

  return (
    <div className="dynamodb-operation-stack">
      <JSONOperationForm
        buttonLabel="Create table"
        disabled={busyOperation === 'create-table'}
        json={createTableJSON}
        label="CreateTable input"
        onChange={setCreateTableJSON}
        onSubmit={onCreateTable}
      />
      <JSONOperationForm
        buttonLabel="Put item"
        disabled={tableActionDisabled || busyOperation === 'put-item'}
        json={putItemJSON}
        label="PutItem input"
        onChange={setPutItemJSON}
        onSubmit={onPutItem}
      />
      <JSONOperationForm
        buttonLabel="Update item"
        disabled={tableActionDisabled || busyOperation === 'update-item'}
        json={updateItemJSON}
        label="UpdateItem input"
        onChange={setUpdateItemJSON}
        onSubmit={onUpdateItem}
      />
      <JSONOperationForm
        buttonLabel="Update TTL"
        disabled={tableActionDisabled || busyOperation === 'ttl'}
        json={ttlJSON}
        label="UpdateTimeToLive input"
        onChange={setTTLJSON}
        onSubmit={onUpdateTTL}
      />
      <JSONOperationForm
        buttonClassName="danger"
        buttonLabel="Delete item"
        confirmation={deleteItemConfirmation}
        destructiveAcknowledgement={deleteItemAcknowledged}
        destructiveAcknowledgementLabel="Step 1: I understand this deletes the item identified by the JSON key."
        confirmationLabel={`Type ${activeTableName ?? 'table name'} to delete item`}
        disabled={
          tableActionDisabled ||
          !deleteItemAcknowledged ||
          deleteItemConfirmation !== activeTableName ||
          busyOperation === 'delete-item'
        }
        json={deleteItemJSON}
        label="DeleteItem input"
        onChange={setDeleteItemJSON}
        onDestructiveAcknowledgementChange={setDeleteItemAcknowledged}
        onConfirmationChange={setDeleteItemConfirmation}
        onSubmit={onDeleteItem}
      />
      <JSONOperationForm
        buttonClassName="danger"
        buttonLabel="Delete table"
        confirmation={deleteTableConfirmation}
        destructiveAcknowledgement={deleteTableAcknowledged}
        destructiveAcknowledgementLabel="Step 1: I understand this deletes the selected table and its local items."
        confirmationLabel={`Type ${activeTableName ?? 'table name'} to delete table`}
        disabled={
          tableActionDisabled ||
          !deleteTableAcknowledged ||
          deleteTableConfirmation !== activeTableName ||
          busyOperation === 'delete-table'
        }
        json={deleteTableJSON}
        label="DeleteTable input"
        onChange={setDeleteTableJSON}
        onDestructiveAcknowledgementChange={setDeleteTableAcknowledged}
        onConfirmationChange={setDeleteTableConfirmation}
        onSubmit={onDeleteTable}
      />
    </div>
  )
}

type JSONOperationFormProps = {
  buttonClassName?: string
  buttonLabel: string
  confirmation?: string
  confirmationLabel?: string
  destructiveAcknowledgement?: boolean
  destructiveAcknowledgementLabel?: string
  disabled: boolean
  json: string
  label: string
  onChange: (value: string) => void
  onConfirmationChange?: (value: string) => void
  onDestructiveAcknowledgementChange?: (value: boolean) => void
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
}

function JSONOperationForm({
  buttonClassName,
  buttonLabel,
  confirmation,
  confirmationLabel,
  destructiveAcknowledgement,
  destructiveAcknowledgementLabel,
  disabled,
  json,
  label,
  onChange,
  onConfirmationChange,
  onDestructiveAcknowledgementChange,
  onSubmit,
}: JSONOperationFormProps): JSX.Element {
  return (
    <form className="dynamodb-operation-form" onSubmit={onSubmit}>
      <label className="redshift-sql-editor">
        <span>{label}</span>
        <textarea
          aria-label={label}
          onChange={(event) => onChange(event.target.value)}
          spellCheck={false}
          value={json}
        />
      </label>
      {onDestructiveAcknowledgementChange ? (
        <label className="destructive-confirmation">
          <input
            aria-label={destructiveAcknowledgementLabel}
            checked={Boolean(destructiveAcknowledgement)}
            onChange={(event) => onDestructiveAcknowledgementChange(event.target.checked)}
            type="checkbox"
          />
          <span>{destructiveAcknowledgementLabel}</span>
        </label>
      ) : null}
      {onConfirmationChange ? (
        <label className="compact-filter">
          <span>Step 2: {confirmationLabel}</span>
          <input
            aria-label={confirmationLabel}
            onChange={(event) => onConfirmationChange(event.target.value)}
            value={confirmation ?? ''}
          />
        </label>
      ) : null}
      <Button className={buttonClassName} disabled={disabled} type="submit">
        {buttonLabel}
      </Button>
    </form>
  )
}
