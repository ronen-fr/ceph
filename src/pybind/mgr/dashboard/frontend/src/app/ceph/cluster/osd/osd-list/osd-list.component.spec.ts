import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import * as _ from 'lodash';
import { BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { EMPTY, of } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../../testing/unit-test-helper';
import { CoreModule } from '../../../../core/core.module';
import { OsdService } from '../../../../shared/api/osd.service';
import { ConfirmationModalComponent } from '../../../../shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permissions } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { CephModule } from '../../../ceph.module';
import { PerformanceCounterModule } from '../../../performance-counter/performance-counter.module';
import { OsdReweightModalComponent } from '../osd-reweight-modal/osd-reweight-modal.component';
import { OsdListComponent } from './osd-list.component';

describe('OsdListComponent', () => {
  let component: OsdListComponent;
  let fixture: ComponentFixture<OsdListComponent>;
  let modalServiceShowSpy: jasmine.Spy;
  let osdService: OsdService;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({
        'config-opt': ['read', 'update', 'create', 'delete'],
        osd: ['read', 'update', 'create', 'delete']
      });
    }
  };

  const getTableAction = (name) => component.tableActions.find((action) => action.name === name);

  const setFakeSelection = () => {
    // Default data and selection
    const selection = [{ id: 1 }];
    const data = [{ id: 1 }];

    // Table data and selection
    component.selection = new CdTableSelection();
    component.selection.selected = selection;
    component.selection.update();
    component.osds = data;
    component.permissions = fakeAuthStorageService.getPermissions();
  };

  const openActionModal = (actionName) => {
    setFakeSelection();
    getTableAction(actionName).click();
  };

  /**
   * The following modals are called after the information about their
   * safety to destroy/remove/mark them lost has been retrieved, hence
   * we will have to fake its request to be able to open those modals.
   */
  const mockSafeToDestroy = () => {
    spyOn(TestBed.get(OsdService), 'safeToDestroy').and.callFake(() =>
      of({ 'safe-to-destroy': true })
    );
  };

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      PerformanceCounterModule,
      TabsModule.forRoot(),
      CephModule,
      ReactiveFormsModule,
      RouterTestingModule,
      CoreModule
    ],
    declarations: [],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      TableActionsComponent,
      BsModalService,
      i18nProviders
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdListComponent);
    component = fixture.componentInstance;
    osdService = TestBed.get(OsdService);
    modalServiceShowSpy = spyOn(TestBed.get(BsModalService), 'show').and.stub();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    fixture.detectChanges();
    expect(
      component.columns
        .filter((column) => !column.checkboxable)
        .every((column) => Boolean(column.prop))
    ).toBeTruthy();
  });

  describe('getOsdList', () => {
    let osds;

    const createOsd = (n: number) => ({
      in: 'in',
      up: 'up',
      stats_history: {
        op_out_bytes: [[n, n], [n * 2, n * 2]],
        op_in_bytes: [[n * 3, n * 3], [n * 4, n * 4]]
      },
      stats: {
        stat_bytes_used: n * n,
        stat_bytes: n * n * n
      },
      state: []
    });

    const expectAttributeOnEveryOsd = (attr: string) =>
      expect(component.osds.every((osd) => Boolean(_.get(osd, attr)))).toBeTruthy();

    beforeEach(() => {
      spyOn(osdService, 'getList').and.callFake(() => of(osds));
      osds = [createOsd(1), createOsd(2), createOsd(3)];
      component.getOsdList();
    });

    it('should replace "this.osds" with new data', () => {
      expect(component.osds.length).toBe(3);
      expect(osdService.getList).toHaveBeenCalledTimes(1);

      osds = [createOsd(4)];
      component.getOsdList();
      expect(component.osds.length).toBe(1);
      expect(osdService.getList).toHaveBeenCalledTimes(2);
    });

    it('should have custom attribute "collectedStates"', () => {
      expectAttributeOnEveryOsd('collectedStates');
      expect(component.osds[0].collectedStates).toEqual(['in', 'up']);
    });

    it('should have "destroyed" state in "collectedStates"', () => {
      osds[0].state.push('destroyed');
      osds[0].up = 0;
      component.getOsdList();

      expectAttributeOnEveryOsd('collectedStates');
      expect(component.osds[0].collectedStates).toEqual(['in', 'destroyed']);
    });

    it('should have custom attribute "stats_history.out_bytes"', () => {
      expectAttributeOnEveryOsd('stats_history.out_bytes');
      expect(component.osds[0].stats_history.out_bytes).toEqual([1, 2]);
    });

    it('should have custom attribute "stats_history.in_bytes"', () => {
      expectAttributeOnEveryOsd('stats_history.in_bytes');
      expect(component.osds[0].stats_history.in_bytes).toEqual([3, 4]);
    });

    it('should have custom attribute "stats.usage"', () => {
      expectAttributeOnEveryOsd('stats.usage');
      expect(component.osds[0].stats.usage).toBe(1);
      expect(component.osds[1].stats.usage).toBe(0.5);
      expect(component.osds[2].stats.usage).toBe(3 / 9);
    });

    it('should have custom attribute "cdIsBinary" to be true', () => {
      expectAttributeOnEveryOsd('cdIsBinary');
      expect(component.osds[0].cdIsBinary).toBe(true);
    });
  });

  describe('show osd actions as defined', () => {
    const getOsdActions = () => {
      fixture.detectChanges();
      return fixture.debugElement.query(By.css('#cluster-wide-actions')).componentInstance
        .dropDownActions;
    };

    it('shows osd actions after osd-actions', () => {
      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css('#cluster-wide-actions'))).toBe(
        fixture.debugElement.queryAll(By.directive(TableActionsComponent))[1]
      );
    });

    it('shows both osd actions', () => {
      const osdActions = getOsdActions();
      expect(osdActions).toEqual(component.clusterWideActions);
      expect(osdActions.length).toBe(3);
    });

    it('shows only "Flags" action', () => {
      component.permissions.configOpt.read = false;
      const osdActions = getOsdActions();
      expect(osdActions[0].name).toBe('Flags');
      expect(osdActions.length).toBe(1);
    });

    it('shows only "Recovery Priority" action', () => {
      component.permissions.osd.read = false;
      const osdActions = getOsdActions();
      expect(osdActions[0].name).toBe('Recovery Priority');
      expect(osdActions[1].name).toBe('PG scrub');
      expect(osdActions.length).toBe(2);
    });

    it('shows no osd actions', () => {
      component.permissions.configOpt.read = false;
      component.permissions.osd.read = false;
      const osdActions = getOsdActions();
      expect(osdActions).toEqual([]);
    });
  });

  it('should test all TableActions combinations', () => {
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permissions.osd);
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: [
          'Create',
          'Scrub',
          'Deep Scrub',
          'Reweight',
          'Mark Out',
          'Mark In',
          'Mark Down',
          'Mark Lost',
          'Purge',
          'Destroy'
        ],
        primary: { multiple: 'Scrub', executing: 'Scrub', single: 'Scrub', no: 'Create' }
      },
      'create,update': {
        actions: ['Create', 'Scrub', 'Deep Scrub', 'Reweight', 'Mark Out', 'Mark In', 'Mark Down'],
        primary: { multiple: 'Scrub', executing: 'Scrub', single: 'Scrub', no: 'Create' }
      },
      'create,delete': {
        actions: ['Create', 'Mark Lost', 'Purge', 'Destroy'],
        primary: {
          multiple: 'Create',
          executing: 'Mark Lost',
          single: 'Mark Lost',
          no: 'Create'
        }
      },
      create: {
        actions: ['Create'],
        primary: { multiple: 'Create', executing: 'Create', single: 'Create', no: 'Create' }
      },
      'update,delete': {
        actions: [
          'Scrub',
          'Deep Scrub',
          'Reweight',
          'Mark Out',
          'Mark In',
          'Mark Down',
          'Mark Lost',
          'Purge',
          'Destroy'
        ],
        primary: { multiple: 'Scrub', executing: 'Scrub', single: 'Scrub', no: 'Scrub' }
      },
      update: {
        actions: ['Scrub', 'Deep Scrub', 'Reweight', 'Mark Out', 'Mark In', 'Mark Down'],
        primary: { multiple: 'Scrub', executing: 'Scrub', single: 'Scrub', no: 'Scrub' }
      },
      delete: {
        actions: ['Mark Lost', 'Purge', 'Destroy'],
        primary: {
          multiple: 'Mark Lost',
          executing: 'Mark Lost',
          single: 'Mark Lost',
          no: 'Mark Lost'
        }
      },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });

  describe('test table actions in submenu', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    beforeEach(fakeAsync(() => {
      // The menu needs a click to render the dropdown!
      const dropDownToggle = fixture.debugElement.query(By.css('.dropdown-toggle'));
      dropDownToggle.triggerEventHandler('click', null);
      tick();
      fixture.detectChanges();
    }));

    it('has all menu entries disabled except create', () => {
      const tableActionElement = fixture.debugElement.query(By.directive(TableActionsComponent));
      const toClassName = TestBed.get(TableActionsComponent).toClassName;
      const getActionClasses = (action: CdTableAction) =>
        tableActionElement.query(By.css(`.${toClassName(action.name)} .dropdown-item`)).classes;

      component.tableActions.forEach((action) => {
        if (action.name === 'Create') {
          return;
        }
        expect(getActionClasses(action).disabled).toBe(true);
      });
    });
  });

  describe('tests if all modals are opened correctly', () => {
    /**
     * Helper function to check if a function opens a modal
     *
     * @param modalClass - The expected class of the modal
     */
    const expectOpensModal = (actionName: string, modalClass): void => {
      openActionModal(actionName);

      // @TODO: check why tsc is complaining when passing 'expectationFailOutput' param.
      expect(modalServiceShowSpy.calls.any()).toBeTruthy();
      expect(modalServiceShowSpy.calls.first().args[0]).toBe(modalClass);

      modalServiceShowSpy.calls.reset();
    };

    it('opens the reweight modal', () => {
      expectOpensModal('Reweight', OsdReweightModalComponent);
    });

    it('opens all confirmation modals', () => {
      const modalClass = ConfirmationModalComponent;
      expectOpensModal('Mark Out', modalClass);
      expectOpensModal('Mark In', modalClass);
      expectOpensModal('Mark Down', modalClass);
    });

    it('opens all critical confirmation modals', () => {
      const modalClass = CriticalConfirmationModalComponent;
      mockSafeToDestroy();
      expectOpensModal('Mark Lost', modalClass);
      expectOpensModal('Purge', modalClass);
      expectOpensModal('Destroy', modalClass);
    });
  });

  describe('tests if the correct methods are called on confirmation', () => {
    const expectOsdServiceMethodCalled = (
      actionName: string,
      osdServiceMethodName: 'markOut' | 'markIn' | 'markDown' | 'markLost' | 'purge' | 'destroy'
    ): void => {
      const osdServiceSpy = spyOn(osdService, osdServiceMethodName).and.callFake(() => EMPTY);
      openActionModal(actionName);
      const initialState = modalServiceShowSpy.calls.first().args[1].initialState;
      const submit = initialState.onSubmit || initialState.submitAction;
      submit.call(component);

      expect(osdServiceSpy.calls.count()).toBe(1);
      expect(osdServiceSpy.calls.first().args[0]).toBe(1);

      // Reset spies to be able to recreate them
      osdServiceSpy.calls.reset();
      modalServiceShowSpy.calls.reset();
    };

    it('calls the corresponding service methods in confirmation modals', () => {
      expectOsdServiceMethodCalled('Mark Out', 'markOut');
      expectOsdServiceMethodCalled('Mark In', 'markIn');
      expectOsdServiceMethodCalled('Mark Down', 'markDown');
    });

    it('calls the corresponding service methods in critical confirmation modals', () => {
      mockSafeToDestroy();
      expectOsdServiceMethodCalled('Mark Lost', 'markLost');
      expectOsdServiceMethodCalled('Purge', 'purge');
      expectOsdServiceMethodCalled('Destroy', 'destroy');
    });
  });
});
