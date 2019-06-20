import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

export class AppConstants {
  public static readonly organization = 'ceph';
  public static readonly projectName = 'Ceph Manager Dashboard';
  public static readonly license = 'Free software (LGPL 2.1).';
}

export enum URLVerbs {
  /* Create a new item */
  CREATE = 'create',

  /* Make changes to an existing item */
  EDIT = 'edit',

  /* Make changes to an existing item */
  UPDATE = 'update',

  /* Remove an item from a container WITHOUT deleting it */
  REMOVE = 'remove',

  /* Destroy an existing item */
  DELETE = 'delete',

  /* Add an existing item to a container */
  ADD = 'add',

  /* Non-standard verbs */
  COPY = 'copy',
  CLONE = 'clone'
}

export enum ActionLabels {
  /* Create a new item */
  CREATE = 'Create',

  /* Destroy an existing item */
  DELETE = 'Delete',

  /* Add an existing item to a container */
  ADD = 'Add',

  /* Remove an item from a container WITHOUT deleting it */
  REMOVE = 'Remove',

  /* Make changes to an existing item */
  EDIT = 'Edit',

  /* */
  CANCEL = 'Cancel',

  /* Non-standard actions */
  COPY = 'Copy',
  CLONE = 'Clone',

  /* Read-only */
  SHOW = 'Show'
}

@Injectable({
  providedIn: 'root'
})
export class ActionLabelsI18n {
  /* This service is required as the i18n polyfill does not provide static
  translation
  */
  CREATE: string;
  DELETE: string;
  ADD: string;
  REMOVE: string;
  EDIT: string;
  CANCEL: string;
  COPY: string;
  CLONE: string;
  DEEP_SCRUB: string;
  DESTROY: string;
  FLATTEN: string;
  MARK_DOWN: string;
  MARK_IN: string;
  MARK_LOST: string;
  MARK_OUT: string;
  PROTECT: string;
  PURGE: string;
  RENAME: string;
  RESTORE: string;
  REWEIGHT: string;
  ROLLBACK: string;
  SCRUB: string;
  SHOW: string;
  TRASH: string;
  UNPROTECT: string;

  constructor(private i18n: I18n) {
    /* Create a new item */
    this.CREATE = this.i18n('Create');

    /* Destroy an existing item */
    this.DELETE = this.i18n('Delete');

    /* Add an existing item to a container */
    this.ADD = this.i18n('Add');

    /* Remove an item from a container WITHOUT deleting it */
    this.REMOVE = this.i18n('Remove');

    /* Make changes to an existing item */
    this.EDIT = this.i18n('Edit');
    this.CANCEL = this.i18n('Cancel');

    /* Non-standard actions */
    this.CLONE = this.i18n('Clone');
    this.COPY = this.i18n('Copy');
    this.DEEP_SCRUB = this.i18n('Deep Scrub');
    this.DESTROY = this.i18n('Destroy');
    this.FLATTEN = this.i18n('Flatten');
    this.MARK_DOWN = this.i18n('Mark Down');
    this.MARK_IN = this.i18n('Mark In');
    this.MARK_LOST = this.i18n('Mark Lost');
    this.MARK_OUT = this.i18n('Mark Out');
    this.PROTECT = this.i18n('Protect');
    this.PURGE = this.i18n('Purge');
    this.RENAME = this.i18n('Rename');
    this.RESTORE = this.i18n('Restore');
    this.REWEIGHT = this.i18n('Reweight');
    this.ROLLBACK = this.i18n('Rollback');
    this.SCRUB = this.i18n('Scrub');
    this.SHOW = this.i18n('Show');
    this.TRASH = this.i18n('Move to Trash');
    this.UNPROTECT = this.i18n('Unprotect');
  }
}
