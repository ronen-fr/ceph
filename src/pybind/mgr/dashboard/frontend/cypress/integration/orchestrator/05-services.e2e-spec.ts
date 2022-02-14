import { ServicesPageHelper } from '../cluster/services.po';

describe('Services page', () => {
  const services = new ServicesPageHelper();
  const serviceName = 'rgw.foo';

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    services.navigateTo();
  });

  describe('when Orchestrator is available', () => {
    it('should create an rgw service', () => {
      services.navigateTo('create');
      services.addService('rgw');

      services.checkExist(serviceName, true);
    });

    it('should edit a service', () => {
      const count = '2';
      services.editService(serviceName, count);
      services.expectPlacementCount(serviceName, count);
    });

    it('should create and delete an ingress service', () => {
      services.navigateTo('create');
      services.addService('ingress');

      services.checkExist('ingress.rgw.foo', true);

      services.deleteService('ingress.rgw.foo');
    });

    it('should create and delete snmp-gateway service with version V2c', () => {
      services.navigateTo('create');
      services.addService('snmp-gateway', false, '1', 'V2c');
      services.checkExist('snmp-gateway', true);

      services.clickServiceTab('snmp-gateway', 'Details');
      cy.get('cd-service-details').within(() => {
        services.checkServiceStatus('snmp-gateway');
      });

      services.deleteService('snmp-gateway');
    });

    it('should create and delete snmp-gateway service with version V3', () => {
      services.navigateTo('create');
      services.addService('snmp-gateway', false, '1', 'V3');
      services.checkExist('snmp-gateway', true);

      services.clickServiceTab('snmp-gateway', 'Details');
      cy.get('cd-service-details').within(() => {
        services.checkServiceStatus('snmp-gateway');
      });

      services.deleteService('snmp-gateway');
    });

    it('should create and delete snmp-gateway service with version V3 and w/o privacy protocol', () => {
      services.navigateTo('create');
      services.addService('snmp-gateway', false, '1', 'V3', false);
      services.checkExist('snmp-gateway', true);

      services.clickServiceTab('snmp-gateway', 'Details');
      cy.get('cd-service-details').within(() => {
        services.checkServiceStatus('snmp-gateway');
      });

      services.deleteService('snmp-gateway');
    });
  });
});
